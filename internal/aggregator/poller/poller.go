// Package poller is responsible for discovering Kubernetes resources and calculating their current, raw operational status.
// It acts as the "Reader" of the cluster state for the Aggregator, without any knowledge of historical events or storage.
package poller

import (
	"context"
	"fmt"

	"example.com/megamon/internal/aggregator/utils"
	"example.com/megamon/internal/k8sutils"
	"example.com/megamon/internal/records"
	containerv1beta1 "google.golang.org/api/container/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

var log = logf.Log.WithName("upness")

type GKEClient interface {
	ListNodePools(ctx context.Context) ([]*containerv1beta1.NodePool, error)
}

type Poller struct {
	Client                 client.Client
	GKE                    GKEClient
	SliceEnabled           bool
	LeaderWorkerSetEnabled bool
}

func NewPoller(c client.Client, gke GKEClient, sliceEnabled bool, lwsEnabled bool) *Poller {
	return &Poller{
		Client:                 c,
		GKE:                    gke,
		SliceEnabled:           sliceEnabled,
		LeaderWorkerSetEnabled: lwsEnabled,
	}
}

func (p *Poller) PollResources(ctx context.Context, report *records.Report) error {

	var jobsetList jobset.JobSetList
	if err := p.Client.List(ctx, &jobsetList); err != nil {
		return fmt.Errorf("listing jobsets: %w", err)
	}

	uidMapKey := func(ns, name string) string {
		return fmt.Sprintf("%s/%s", ns, name)
	}
	// map[<ns>/<name>]<uid>
	uidMap := map[string]string{}

	var lwsList lws.LeaderWorkerSetList
	if p.LeaderWorkerSetEnabled {
		if err := p.Client.List(ctx, &lwsList); err != nil {
			return fmt.Errorf("listing leaderworkersets: %w", err)
		}
	}

	if p.LeaderWorkerSetEnabled {
		for _, lwsObj := range lwsList.Items {
			uid := string(lwsObj.UID)
			attrs := utils.ExtractLeaderWorkerSetAttrs(&lwsObj)

			expectedReplicas := int32(1)
			if lwsObj.Spec.Replicas != nil {
				expectedReplicas = *lwsObj.Spec.Replicas
			}

			report.LeaderWorkerSetsUp[uid] = records.Upness{
				ExpectedCount: expectedReplicas,
				ReadyCount:    lwsObj.Status.ReadyReplicas,
				Attrs:         attrs,
			}
		}
	}

	for _, js := range jobsetList.Items {
		if js.Status.TerminalState != "" {
			log.Info("jobset terminal state", "jobset", js.Name, "state", js.Status.TerminalState)
		}

		uid := string(js.UID)
		uidMap[uidMapKey(js.Namespace, js.Name)] = uid

		attrs := utils.ExtractJobSetAttrs(&js)
		specReplicas, readyReplicas := k8sutils.GetJobSetReplicas(&js)

		state, isTerminal := k8sutils.GetJobSetTerminalState(&js)
		expectedDown := false
		if isTerminal {
			if state == jobset.JobSetCompleted {
				expectedDown = true
			}
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

	var nodeList corev1.NodeList
	if err := p.Client.List(ctx, &nodeList); err != nil {
		return fmt.Errorf("listing nodes: %w", err)
	}

	npList, err := p.GKE.ListNodePools(ctx)
	if err != nil {
		return fmt.Errorf("listing node pools: %w", err)
	}
	for _, np := range npList {
		func() {
			if !utils.IsTPUNodePool(np) {
				return
			}
			up := records.Upness{
				Attrs:        utils.ExtractNodePoolAttrs(np),
				Status:       np.Status,
				ExpectedDown: np.Status == "STOPPING" || np.Status == "DELETING",
			}
			expectedCount, err := utils.GetExpectedTPUNodePoolSize(np)
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
		if !p.SliceEnabled {
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

	return nil
}
