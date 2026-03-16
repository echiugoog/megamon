package controller

import (
	"context"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	slicev1beta1 "example.com/megamon/copied-slice-api/v1beta1"
	"example.com/megamon/internal/aggregator"
	"example.com/megamon/internal/aggregator/utils"
	"example.com/megamon/internal/k8sutils"
	"example.com/megamon/internal/records"
)

const (
	ownerKindJobSet          = "jobset"
	ownerKindLeaderWorkerSet = "leaderworkerset"
)

// SliceReconciler reconciles a Slice object
type SliceReconciler struct {
	Name string
	client.Client
	Scheme                 *runtime.Scheme
	EventReconciler        aggregator.EventReconciler
	LeaderWorkerSetEnabled bool

	slicesMtx sync.RWMutex
	slicesUp  map[string]records.SliceState
}

// TODO Remove me, this gets patched via kustomize
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices/status,verbs=get

func (r *SliceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	// List ALL slices to get the current state for batch reconciliation
	var sliceList slicev1beta1.SliceList
	if err := r.List(ctx, &sliceList); err != nil {
		lg.Error(err, "failed to list slices")
		return ctrl.Result{}, err
	}

	// 1. Calculate current Upness for ALL slices
	ups := make(map[string]records.SliceState)
	for _, s := range sliceList.Items {
		attrs := utils.ExtractSliceAttrs(&s)

		ownerKind := strings.ToLower(attrs.SliceOwnerKind)
		if ownerKind != ownerKindJobSet && ownerKind != ownerKindLeaderWorkerSet {
			lg.Error(nil, "Slice owner-kind is unsupported, ignoring slice", "name", s.Name, "kind", attrs.SliceOwnerKind)
			continue
		}

		// Fetch owner status to determine if downtime is "Expected"
		ownerExists, ownerTerminal, err := r.getOwnerStatus(ctx, ownerKind, attrs.SliceOwnerName, attrs.SliceOwnerNamespace)
		if err != nil {
			lg.Error(err, "Error looking up slice owner", "sliceOwner", attrs.SliceOwnerName, "namespace", attrs.SliceOwnerNamespace)
			return ctrl.Result{}, err
		}

		isTerminating := !s.DeletionTimestamp.IsZero()
		expectedDown := isTerminating && (!ownerExists || ownerTerminal)

		// If the slice is active but its owner is gone/terminal, it's also expected to be down
		if !isTerminating && (!ownerExists || ownerTerminal) {
			expectedDown = true
		}

		up := records.Upness{
			Attrs:         attrs,
			ExpectedCount: 1,
			ExpectedDown:  expectedDown,
		}

		if !isTerminating {
			for _, cond := range s.Status.Conditions {
				if cond.Type == slicev1beta1.SliceStateConditionType {
					switch cond.Status {
					case metav1.ConditionTrue:
						up.ReadyCount = 1
					case metav1.ConditionUnknown:
						up.UnknownCount = 1
					}
					break
				}
			}
		}

		ups[s.Name] = records.SliceState{Upness: up, Deleted: isTerminating}
	}

	// 2. Directly perform GCS Reconciliation for Slices
	// We use the Aggregator's EventReconciler instance
	now := time.Now()
	slicesContext := log.IntoContext(ctx, lg.WithValues("type", "slices-batch"))

	upnessMap := make(map[string]records.Upness)
	for k, v := range ups {
		upnessMap[k] = v.Upness
	}

	_, err := r.EventReconciler.Reconcile(slicesContext, now, "slices.json", upnessMap)

	if err != nil {
		lg.Error(err, "failed to reconcile slice events to GCS")
		return ctrl.Result{}, err
	}

	// 3. Update the in-memory upness map for ALL slices
	// This ensures the summary producer has the latest current state.
	r.SetAllSliceUpness(ups)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SliceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	name := "slice"
	if r.Name != "" {
		name = r.Name
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&slicev1beta1.Slice{}).
		Named(name).
		Complete(r)
}

func (r *SliceReconciler) getOwnerStatus(ctx context.Context, ownerKind, ownerName, ownerNamespace string) (bool, bool, error) {
	if ownerName == "" || ownerNamespace == "" {
		return false, false, nil
	}

	switch ownerKind {
	case ownerKindJobSet:
		var js jobset.JobSet
		if err := r.Get(ctx, types.NamespacedName{Name: ownerName, Namespace: ownerNamespace}, &js); err == nil {
			_, ownerTerminal := k8sutils.GetJobSetTerminalState(&js)
			return true, ownerTerminal, nil
		} else if !errors.IsNotFound(err) {
			return false, false, err
		}
	case ownerKindLeaderWorkerSet:
		if r.LeaderWorkerSetEnabled {
			var lwsObj lws.LeaderWorkerSet
			if err := r.Get(ctx, types.NamespacedName{Name: ownerName, Namespace: ownerNamespace}, &lwsObj); err == nil {
				_, ownerTerminal := k8sutils.GetLeaderWorkerSetTerminalState(&lwsObj)
				return true, ownerTerminal, nil
			} else if !errors.IsNotFound(err) {
				return false, false, err
			}
		}
	}

	return false, false, nil
}

func (r *SliceReconciler) GetSlices() map[string]records.SliceState {
	r.slicesMtx.RLock()
	defer r.slicesMtx.RUnlock()
	cp := make(map[string]records.SliceState, len(r.slicesUp))
	for k, v := range r.slicesUp {
		cp[k] = v
	}
	return cp
}

func (r *SliceReconciler) DeleteSlice(name string) {
	r.slicesMtx.Lock()
	defer r.slicesMtx.Unlock()
	delete(r.slicesUp, name)
}

func (r *SliceReconciler) SetAllSliceUpness(ups map[string]records.SliceState) {
	r.slicesMtx.Lock()
	defer r.slicesMtx.Unlock()
	r.slicesUp = ups
}
