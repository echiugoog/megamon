package aggregator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"example.com/megamon/internal/metrics"
	"example.com/megamon/internal/records"
	containerv1beta1 "google.golang.org/api/container/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("aggregator")

type GKEClient interface {
	ListNodePools(ctx context.Context) ([]*containerv1beta1.NodePool, error)
}

type Aggregator struct {
	client.Client

	ResourcePoller  ResourcePoller
	EventStore      EventStore
	EventReconciler EventReconciler
	SummaryProducer SummaryProducer

	EventsBucketName string
	EventsBucketPath string

	AggregationInterval    time.Duration
	PollingInterval        time.Duration
	UnknownCountThreshold  float64
	SliceEnabled           bool
	LeaderWorkerSetEnabled bool

	reportMtx   sync.RWMutex
	report      records.Report
	reportReady bool

	polledReportMtx sync.RWMutex
	polledReport    records.Report

	SliceProvider records.SliceProvider

	nodePoolSchedulingMtx sync.RWMutex
	// map[<nodepool-name>]<details-about-what-is-scheduled-on-it>
	nodePoolScheduling map[string]records.ScheduledJob

	Exporters map[string]Exporter
	GKE       GKEClient
}

func (a *Aggregator) Start(ctx context.Context) error {
	if a.PollingInterval <= 0 {
		a.PollingInterval = a.AggregationInterval
		log.Info("polling interval not set, defaulting to aggregation interval", "pollingInterval", a.PollingInterval)
	}

	log.Info("starting aggregator", "aggregationInterval", a.AggregationInterval, "pollingInterval", a.PollingInterval)

	// Optional decoupled polling loop for reading resource states independently.
	go func() {
		t := time.NewTicker(a.PollingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				log.V(3).Info("polling resources")
				newReport := records.NewReport()
				if err := a.ResourcePoller.PollResources(ctx, &newReport); err != nil {
					log.Error(err, "failed to poll resources")
					continue
				}
				a.polledReportMtx.Lock()
				a.polledReport = newReport
				a.polledReportMtx.Unlock()
			}
		}
	}()

	// Main aggregation loop that calculates and reports metrics.
	t := time.NewTicker(a.AggregationInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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

func (a *Aggregator) ReportReady() bool {
	a.reportMtx.RLock()
	defer a.reportMtx.RUnlock()
	return a.reportReady
}

func (a *Aggregator) Init() {
	a.nodePoolScheduling = make(map[string]records.ScheduledJob)
}

func (a *Aggregator) Aggregate(ctx context.Context) error {
	a.polledReportMtx.RLock()
	report := a.polledReport.Clone()
	a.polledReportMtx.RUnlock()

	now := time.Now()
	var err error

	jobsetContext := logf.IntoContext(ctx, log.WithValues("type", "jobsets"))
	jobsetNodesContext := logf.IntoContext(ctx, log.WithValues("type", "jobset-nodes"))
	lwsContext := logf.IntoContext(ctx, log.WithValues("type", "leader-worker-sets"))
	nodePoolsContext := logf.IntoContext(ctx, log.WithValues("type", "nodepools"))

	// 1. Reconcile JobSets, NodePools, and LWS (ignore returned records)
	_, err = a.EventReconciler.Reconcile(jobsetContext, now, "jobsets.json", report.JobSetsUp)
	if err != nil {
		return fmt.Errorf("reconciling jobset events: %w", err)
	}
	// jobsetnodes only reconciled if slice is not enabled
	if !a.SliceEnabled {
		_, err = a.EventReconciler.Reconcile(jobsetNodesContext, now, "jobset-nodes.json", report.JobSetNodesUp)
		if err != nil {
			return fmt.Errorf("reconciling jobset node events: %w", err)
		}
	}
	_, err = a.EventReconciler.Reconcile(nodePoolsContext, now, "node-pools.json", report.NodePoolsUp)
	if err != nil {
		return fmt.Errorf("reconciling nodepool events: %w", err)
	}
	if a.LeaderWorkerSetEnabled {
		_, err = a.EventReconciler.Reconcile(lwsContext, now, "leader-worker-sets.json", report.LeaderWorkerSetsUp)
		if err != nil {
			return fmt.Errorf("reconciling lws events: %w", err)
		}
	}

	// 2. Sync Slice states and prune those with inactive owners
	if a.SliceEnabled {
		a.EventReconciler.SyncSlices(a.SliceProvider, &report)
	}

	// 3. Fetch all events from GCS and Update summaries (consistent source of truth)
	if err := a.SummaryProducer.GenerateSummaries(ctx, now, a.EventStore, a.SliceEnabled, a.LeaderWorkerSetEnabled, &report); err != nil {
		return fmt.Errorf("producing summaries: %w", err)
	}

	a.pruneNodePoolScheduling(report.NodePoolsUp)
	report.NodePoolScheduling = a.getNodePoolScheduling()

	a.reportMtx.Lock()
	a.report = report
	a.reportReady = true
	a.reportMtx.Unlock()

	return nil
}

func (a *Aggregator) SetNodePoolScheduling(nodePoolName string, job records.ScheduledJob) {
	a.nodePoolSchedulingMtx.Lock()
	defer a.nodePoolSchedulingMtx.Unlock()
	if a.nodePoolScheduling == nil {
		a.nodePoolScheduling = make(map[string]records.ScheduledJob)
	}
	a.nodePoolScheduling[nodePoolName] = job
}

func (a *Aggregator) pruneNodePoolScheduling(ups map[string]records.Upness) {
	a.nodePoolSchedulingMtx.Lock()
	defer a.nodePoolSchedulingMtx.Unlock()
	for k := range a.nodePoolScheduling {
		if _, ok := ups[k]; !ok {
			delete(a.nodePoolScheduling, k)
		}
	}
}

func (a *Aggregator) getNodePoolScheduling() map[string]records.ScheduledJob {
	a.nodePoolSchedulingMtx.RLock()
	defer a.nodePoolSchedulingMtx.RUnlock()
	if a.nodePoolScheduling == nil {
		return nil
	}
	cp := make(map[string]records.ScheduledJob, len(a.nodePoolScheduling))
	for k, v := range a.nodePoolScheduling {
		cp[k] = v
	}
	return cp
}
