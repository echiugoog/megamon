package aggregator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"example.com/megamon/internal/k8sutils"
	"example.com/megamon/internal/metrics"
	"example.com/megamon/internal/records"
	containerv1beta1 "google.golang.org/api/container/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

type Aggregator struct {
	client.Client

	EventsBucketName string
	EventsBucketPath string

	Interval               time.Duration
	UnknownCountThreshold  float64
	SliceEnabled           bool
	LeaderWorkerSetEnabled bool

	reportMtx   sync.RWMutex
	report      records.Report
	reportReady bool

	slicesMtx    sync.RWMutex
	SliceUpdates chan SliceUpdate
	slicesUp     map[string]SliceState

	nodePoolSchedulingMtx sync.RWMutex
	// map[<nodepool-name>]<details-about-what-is-scheduled-on-it>
	nodePoolScheduling map[string]records.ScheduledJob

	Exporters map[string]Exporter

	GKE GKEClient
	GCS GCSClient
}

// Config holds the configuration for the Aggregator.
type Config struct {
	Interval               time.Duration
	UnknownCountThreshold  float64
	SliceEnabled           bool
	LeaderWorkerSetEnabled bool
	EventsBucketName       string
	EventsBucketPath       string
}

// NewAggregator creates a new initialized Aggregator.
func NewAggregator(c client.Client, gke GKEClient, gcs GCSClient, cfg Config) *Aggregator {
	agg := &Aggregator{
		Client:                 c,
		GKE:                    gke,
		GCS:                    gcs,
		EventsBucketName:       cfg.EventsBucketName,
		EventsBucketPath:       cfg.EventsBucketPath,
		Interval:               cfg.Interval,
		UnknownCountThreshold:  cfg.UnknownCountThreshold,
		SliceEnabled:           cfg.SliceEnabled,
		LeaderWorkerSetEnabled: cfg.LeaderWorkerSetEnabled,

		report:             records.NewReport(),
		slicesUp:           make(map[string]SliceState),
		nodePoolScheduling: make(map[string]records.ScheduledJob),
		Exporters:          make(map[string]Exporter),
	}

	if cfg.SliceEnabled {
		agg.SliceUpdates = make(chan SliceUpdate, 100)
	}

	return agg
}

type SliceUpdate struct {
	Name   string
	Upness records.Upness
	// Retain = true, to retain metrics instead of dropping them immediately. Will be used when slice gone but owner still active
	Retain bool
	// Delete instructs the aggregator to immediately prune this slice from memory entirely.
	Delete bool
}

type SliceState struct {
	Upness records.Upness
	// Deleted = true when slice is gone but metrics are retained, will continue reporting downtime until its owner is also removed or is terminal.
	Deleted bool
}

var log = logf.Log.WithName("aggregator")

type GKEClient interface {
	ListNodePools(ctx context.Context) ([]*containerv1beta1.NodePool, error)
}

type GCSClient interface {
	GetRecords(ctx context.Context, bucket, path string) (map[string]records.EventRecords, error)
	PutRecords(ctx context.Context, bucket, path string, recs map[string]records.EventRecords) error
}

type Exporter interface {
	Export(context.Context, records.Report) error
}

func (a *Aggregator) ReportReady() bool {
	a.reportMtx.RLock()
	defer a.reportMtx.RUnlock()
	return a.reportReady
}

func (a *Aggregator) Start(ctx context.Context) error {
	t := time.NewTicker(a.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case u := <-a.SliceUpdates:
			a.slicesMtx.Lock()
			if u.Delete {
				log.Info("deleting slice", "name", u.Name)
				delete(a.slicesUp, u.Name)
			} else {
				log.Info("updating slice", "name", u.Name, "SliceState", u)
				a.slicesUp[u.Name] = SliceState{
					Upness:  u.Upness,
					Deleted: u.Retain,
				}
			}
			a.slicesMtx.Unlock()
		case <-t.C:
			log.Info("aggregating")
			start := time.Now()
			if err := a.Aggregate(ctx); err != nil {
				log.Error(err, "failed to aggregate")
				continue
			}
			metrics.AggregationDuration.Record(ctx, time.Since(start).Seconds())

			for name, exporter := range a.Exporters {
				if err := exporter.Export(ctx, a.Report()); err != nil {
					log.Error(err, "failed to export", "exporter", name)
				}
			}
		}
	}
}

func (a *Aggregator) Report() records.Report {
	a.reportMtx.RLock()
	defer a.reportMtx.RUnlock()
	return a.report
}

func (a *Aggregator) GetSliceUpness(name string) (records.Upness, bool) {
	a.slicesMtx.RLock()
	defer a.slicesMtx.RUnlock()
	state, ok := a.slicesUp[name]
	if !ok {
		return records.Upness{}, false
	}
	return state.Upness, true
}

// SetSliceUpness queues a non-blocking update to a slice state.
func (a *Aggregator) SetSliceUpness(name string, up records.Upness, retain bool) {
	a.SliceUpdates <- SliceUpdate{
		Name:   name,
		Upness: up,
		Retain: retain,
	}
}

func (a *Aggregator) DeleteSliceUpness(name string) {
	a.SliceUpdates <- SliceUpdate{
		Name:   name,
		Delete: true,
	}
}

func (a *Aggregator) Aggregate(ctx context.Context) error {
	report := records.NewReport()

	var jobsetList jobset.JobSetList
	if err := a.List(ctx, &jobsetList); err != nil {
		return fmt.Errorf("listing jobsets: %w", err)
	}

	var lwsList lws.LeaderWorkerSetList
	if a.LeaderWorkerSetEnabled {
		if err := a.List(ctx, &lwsList); err != nil {
			return fmt.Errorf("listing leaderworkersets: %w", err)
		}
	}

	now := time.Now()

	uidMapKey := func(ns, name string) string {
		return fmt.Sprintf("%s/%s", ns, name)
	}
	kindOwnerKey := func(kind, ns, name string) string {
		return fmt.Sprintf("%s/%s/%s", strings.ToLower(kind), ns, name)
	}

	// map[<ns>/<name>]<uid>
	uidMap := map[string]string{}

	// activeSliceOwners tracks active owners to determine if a deleted slice's metrics should be retained.
	// map[<owner-kind>/<owner-namespace>/<owner-name>]bool
	var activeSliceOwners map[string]bool
	if a.SliceEnabled {
		activeSliceOwners = map[string]bool{}
	}

	for _, js := range jobsetList.Items {
		if js.Status.TerminalState != "" {
			log.Info("jobset terminal state", "jobset", js.Name, "state", js.Status.TerminalState)
		}

		uid := string(js.UID)
		uidMap[uidMapKey(js.Namespace, js.Name)] = uid

		attrs := extractJobSetAttrs(&js)
		specReplicas, readyReplicas := k8sutils.GetJobSetReplicas(&js)

		state, isTerminal := k8sutils.GetJobSetTerminalState(&js)
		expectedDown := false
		if isTerminal {
			// Completed -> Expected(Planned) Downtime (Not included in TBI)
			// Failed -> Unplanned Downtime (Included in TBI)
			// Suspended -> Unplanned Downtime (Included in TBI)
			if state == jobset.JobSetCompleted {
				expectedDown = true
			}
		}

		if a.SliceEnabled && !isTerminal {
			activeSliceOwners[kindOwnerKey("jobset", js.Namespace, js.Name)] = true
		}

		report.JobSetsUp[uid] = records.Upness{
			ExpectedCount: specReplicas,
			ReadyCount:    readyReplicas,
			Attrs:         attrs,
			Status:        string(state),
			ExpectedDown:  expectedDown,
		}
		report.JobSetNodesUp[uid] = records.Upness{
			ExpectedCount: k8sutils.GetExpectedNodeCount(&js),
			Attrs:         attrs,
		}
	}

	if a.LeaderWorkerSetEnabled {
		for _, lwsObj := range lwsList.Items {
			uid := string(lwsObj.UID)
			attrs := extractLeaderWorkerSetAttrs(&lwsObj)

			expectedReplicas := int32(1)
			if lwsObj.Spec.Replicas != nil {
				expectedReplicas = *lwsObj.Spec.Replicas
			}

			_, isTerminal := k8sutils.GetLeaderWorkerSetTerminalState(&lwsObj)
			if a.SliceEnabled && !isTerminal {
				activeSliceOwners[kindOwnerKey("leaderworkerset", lwsObj.Namespace, lwsObj.Name)] = true
			}

			report.LeaderWorkerSetsUp[uid] = records.Upness{
				ExpectedCount: expectedReplicas,
				ReadyCount:    lwsObj.Status.ReadyReplicas,
				Attrs:         attrs,
			}
		}
	}

	var nodeList corev1.NodeList
	if err := a.List(ctx, &nodeList); err != nil {
		return fmt.Errorf("listing nodes: %w", err)
	}

	jobsetContext := logf.IntoContext(ctx, log.WithValues("type", "jobsets"))
	jobsetNodesContext := logf.IntoContext(ctx, log.WithValues("type", "jobset-nodes"))
	lwsContext := logf.IntoContext(ctx, log.WithValues("type", "leader-worker-sets"))
	nodePoolsContext := logf.IntoContext(ctx, log.WithValues("type", "nodepools"))
	slicesContext := logf.IntoContext(ctx, log.WithValues("type", "slices"))

	var sliceEvents map[string]records.EventRecords
	if a.SliceEnabled {
		a.slicesMtx.Lock()
		for name, state := range a.slicesUp {
			if state.Deleted {
				kind := state.Upness.Attrs.SliceOwnerKind
				ns := state.Upness.Attrs.SliceOwnerNamespace
				ownerName := state.Upness.Attrs.SliceOwnerName

				// Prune the deleted slice's metrics from memory once its owner is also deleted or reaches a terminal state.
				if !activeSliceOwners[kindOwnerKey(kind, ns, ownerName)] {
					delete(a.slicesUp, name)
					continue
				}
			}
			report.SlicesUp[name] = state.Upness
		}
		a.slicesMtx.Unlock()

		var err error
		sliceEvents, err = a.reconcileEvents(slicesContext, now, "slices.json", report.SlicesUp)
		if err != nil {
			return fmt.Errorf("reconciling slice events: %w", err)
		}
	}

	npList, err := a.GKE.ListNodePools(ctx)
	if err != nil {
		return fmt.Errorf("listing node pools: %w", err)
	}
	for _, np := range npList {
		func() {
			if !isTPUNodePool(np) {
				return
			}
			up := records.Upness{
				Attrs:        extractNodePoolAttrs(np),
				Status:       np.Status,
				ExpectedDown: np.Status == "STOPPING" || np.Status == "DELETING",
			}
			expectedCount, err := getExpectedTPUNodePoolSize(np)
			if err != nil {
				log.Error(err, "failed to get expected TPU node pool size", "nodepool", np.Name)
				return
			}
			up.ExpectedCount = expectedCount
			if tpuChipCount, err := k8sutils.GetTpuTopologyToChipCount(up.TPUTopology); err != nil {
				log.Error(err, "failed to convert TPU topology to chip count", "nodepool", np.Name)
			} else {
				up.TPUChipCount = int32(tpuChipCount)
			}
			report.NodePoolsUp[np.Name] = up
		}()
	}

	for _, node := range nodeList.Items {
		nodeStatus := k8sutils.IsNodeReady(&node)

		// Node pool mapping:

		if npName, ok := k8sutils.GetNodePool(&node); ok {
			func() {
				if !k8sutils.IsTPUNode(&node) {
					return
				}
				up, ok := report.NodePoolsUp[npName]
				if !ok {
					log.Info("WARNING: found Node for node pool that was not parsed", "node", node.Name, "nodepool", npName)
					return
				}
				if up.ExpectedCount == 0 {
					var err error
					up.ExpectedCount, err = k8sutils.GetExpectedTPUNodePoolSize(&node)
					if err != nil {
						log.Error(err, "failed to get expected TPU node pool size", "node", node.Name)
						return
					}
				}

				if nodeStatus == corev1.ConditionTrue {
					up.ReadyCount++
				} else if nodeStatus == corev1.ConditionUnknown {
					up.UnknownCount++
				}
				report.NodePoolsUp[npName] = up
			}()
		}

		// Static jobset mapping:
		if !a.SliceEnabled {
			if jsNS, jsName := k8sutils.GetJobSetForNode(&node); jsNS != "" && jsName != "" {
				func() {
					if jsNS == "" || jsName == "" {
						return
					}
					uid, ok := uidMap[uidMapKey(jsNS, jsName)]
					if !ok {
						return
					}

					up, ok := report.JobSetNodesUp[uid]
					if !ok {
						return
					}
					if nodeStatus == corev1.ConditionTrue {
						up.ReadyCount++
					} else if nodeStatus == corev1.ConditionUnknown {
						up.UnknownCount++
					}
					report.JobSetNodesUp[uid] = up
				}()
			}
		}
	}

	log.V(3).Info("DEBUG", "report.NodePoolsUp", report.NodePoolsUp, "report.JobSetNodesUp", report.JobSetNodesUp, "report.JobSetsUp", report.JobSetsUp)
	if a.SliceEnabled {
		log.V(3).Info("DEBUG", "report.SlicesUp", report.SlicesUp)
	}
	if a.LeaderWorkerSetEnabled {
		log.V(3).Info("DEBUG", "report.LeaderWorkerSetsUp", report.LeaderWorkerSetsUp)
	}

	jsEvents, err := a.reconcileEvents(jobsetContext, now, "jobsets.json", report.JobSetsUp)
	if err != nil {
		return fmt.Errorf("reconciling jobset events: %w", err)
	}
	var jsNodeEvents map[string]records.EventRecords
	if !a.SliceEnabled {
		jsNodeEvents, err = a.reconcileEvents(jobsetNodesContext, now, "jobset-nodes.json", report.JobSetNodesUp)
		if err != nil {
			return fmt.Errorf("reconciling jobset node events: %w", err)
		}
	}
	nodePoolEvents, err := a.reconcileEvents(nodePoolsContext, now, "node-pools.json", report.NodePoolsUp)
	if err != nil {
		return fmt.Errorf("reconciling nodepool events: %w", err)
	}

	var lwsEvents map[string]records.EventRecords
	if a.LeaderWorkerSetEnabled {
		lwsEvents, err = a.reconcileEvents(lwsContext, now, "leader-worker-sets.json", report.LeaderWorkerSetsUp)
		if err != nil {
			return fmt.Errorf("reconciling lws events: %w", err)
		}
	}

	for key, events := range jsEvents {
		eventSummary := events.Summarize(jobsetContext, now)
		report.JobSetsUpSummaries[key] = records.UpnessSummaryWithAttrs{
			Attrs:        report.JobSetsUp[key].Attrs,
			EventSummary: eventSummary,
		}
	}
	for key, events := range jsNodeEvents {
		eventSummary := events.Summarize(jobsetNodesContext, now)
		report.JobSetNodesUpSummaries[key] = records.UpnessSummaryWithAttrs{
			Attrs:        report.JobSetNodesUp[key].Attrs,
			EventSummary: eventSummary,
		}
	}
	for key, events := range nodePoolEvents {
		eventSummary := events.Summarize(nodePoolsContext, now)
		report.NodePoolsUpSummaries[key] = records.UpnessSummaryWithAttrs{
			Attrs:        report.NodePoolsUp[key].Attrs,
			EventSummary: eventSummary,
		}
	}
	if a.SliceEnabled {
		for key, events := range sliceEvents {
			eventSummary := events.Summarize(slicesContext, now)
			report.SlicesUpSummaries[key] = records.UpnessSummaryWithAttrs{
				Attrs:        report.SlicesUp[key].Attrs,
				EventSummary: eventSummary,
			}
		}
	}
	if a.LeaderWorkerSetEnabled {
		for key, events := range lwsEvents {
			eventSummary := events.Summarize(lwsContext, now)
			report.LeaderWorkerSetsUpSummaries[key] = records.UpnessSummaryWithAttrs{
				Attrs:        report.LeaderWorkerSetsUp[key].Attrs,
				EventSummary: eventSummary,
			}
		}
	}

	a.pruneNodePoolScheduling(report.NodePoolsUp)
	report.NodePoolScheduling = a.getNodePoolScheduling()

	a.reportMtx.Lock()
	a.report = report
	a.reportReady = true
	a.reportMtx.Unlock()

	return nil
}

func (a *Aggregator) reconcileEvents(ctx context.Context, now time.Time, filename string, ups map[string]records.Upness) (map[string]records.EventRecords, error) {
	path := strings.TrimSuffix(a.EventsBucketPath, "/") + "/" + filename
	recs, err := a.GCS.GetRecords(ctx, a.EventsBucketName, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get %q: %w", filename, err)
	}

	if changed := records.ReconcileEvents(ctx, now, ups, recs, a.UnknownCountThreshold); changed {
		if err := a.GCS.PutRecords(ctx, a.EventsBucketName, path, recs); err != nil {
			return nil, fmt.Errorf("failed to put %q: %w", filename, err)
		}
	}

	return recs, nil
}

func (a *Aggregator) SetNodePoolScheduling(nodePoolName string, job records.ScheduledJob) {
	a.nodePoolSchedulingMtx.Lock()
	defer a.nodePoolSchedulingMtx.Unlock()
	a.nodePoolScheduling[nodePoolName] = job
}

func (a *Aggregator) pruneNodePoolScheduling(nps map[string]records.Upness) {
	a.nodePoolSchedulingMtx.Lock()
	defer a.nodePoolSchedulingMtx.Unlock()
	for npName := range a.nodePoolScheduling {
		if _, ok := nps[npName]; !ok {
			delete(a.nodePoolScheduling, npName)
		}
	}
}

func (a *Aggregator) getNodePoolScheduling() map[string]records.ScheduledJob {
	a.nodePoolSchedulingMtx.RLock()
	defer a.nodePoolSchedulingMtx.RUnlock()
	cp := make(map[string]records.ScheduledJob, len(a.nodePoolScheduling))
	for k, v := range a.nodePoolScheduling {
		cp[k] = v
	}
	return cp
}
