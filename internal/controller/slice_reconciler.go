package controller

import (
	"context"
	"strings"

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
	Scheme     *runtime.Scheme
	Aggregator *aggregator.Aggregator
}

// TODO Remove me, this gets patched via kustomize
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices/status,verbs=get

func (r *SliceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	var slice slicev1beta1.Slice
	if err := r.Get(ctx, req.NamespacedName, &slice); err != nil {
		if errors.IsNotFound(err) {
			// Case: Slice was deleted, check if deletion was expected or unexpected (owner gone/terminal or still active)
			lg.Info("Slice deleted", "name", req.Name, "namespace", req.Namespace)
			prev, exists := r.Aggregator.GetSliceUpness(req.Name)
			if !exists {
				// this is unexpected, log a warning
				lg.Error(nil, "WARNING: No previous state found for slice", "name", req.Name, "namespace", req.Namespace)
				return ctrl.Result{}, nil
			}

			ownerKind := strings.ToLower(prev.Attrs.SliceOwnerKind)
			if ownerKind != ownerKindJobSet && ownerKind != ownerKindLeaderWorkerSet {
				// Case: The slice had an unsupported or missing owner, prune immediately
				lg.Error(nil, "Slice owner-kind is unsupported, deleting metric", "name", req.Name, "kind", prev.Attrs.SliceOwnerKind)
				r.Aggregator.DeleteSliceUpness(req.Name)
				return ctrl.Result{}, nil
			}

			// Check if the owner is still active and if it's terminal
			sliceOwner := prev.Attrs.SliceOwnerName
			sliceOwnerNamespace := prev.Attrs.SliceOwnerNamespace
			ownerExists, ownerTerminal, err := r.getOwnerStatus(ctx, ownerKind, sliceOwner, sliceOwnerNamespace)
			if err != nil {
				lg.Error(err, "Error looking up slice owner", "sliceOwner", sliceOwner, "namespace", sliceOwnerNamespace)
				return ctrl.Result{}, err
			}

			// slice is expected to be down or deleted imminently if it's owner doesn't exist or is in a terminal state
			expectedDown := !ownerExists || ownerTerminal

			if expectedDown {
				// Case: slice expectedDown one, we prune from Upness to avoid false interruption
				r.Aggregator.DeleteSliceUpness(req.Name)
			} else {
				// Case: The slice is gone, but the owner is STILL active and non-terminal, this is an unplanned interruption.
				up := records.Upness{
					Attrs:         prev.Attrs,
					ExpectedCount: 1,
					ReadyCount:    0,
					ExpectedDown:  false,
				}
				r.Aggregator.SetSliceUpness(req.Name, up, true)
			}
			return ctrl.Result{}, nil
		}
		log.Log.Error(err, "failed to get slice", "name", req.Name, "namespace", req.Namespace)
		return ctrl.Result{}, err
	}

	attrs := records.Attrs{
		SliceName:      slice.Name,
		SliceUID:       string(slice.UID),
		TPUAccelerator: string(slice.Spec.Type),
		TPUTopology:    slice.Spec.Topology,
	}

	if slice.Labels != nil {
		if val, ok := slice.Labels[k8sutils.LabelTPUProvisionerOwnerName]; ok {
			attrs.SliceOwnerName = val
		}
		if val, ok := slice.Labels[k8sutils.LabelTPUProvisionerOwnerKind]; ok {
			attrs.SliceOwnerKind = val
		}
		if val, ok := slice.Labels[k8sutils.LabelTPUProvisionerOwnerNamespace]; ok {
			attrs.SliceOwnerNamespace = val
		}
	}

	ownerKind := strings.ToLower(attrs.SliceOwnerKind)
	if ownerKind != ownerKindJobSet && ownerKind != ownerKindLeaderWorkerSet {
		// Case: The slice exists but its owner-kind is missing or unsupported, we log an error, ignore and prune
		lg.Error(nil, "Slice owner-kind is unsupported, ignoring slice", "name", req.Name, "kind", attrs.SliceOwnerKind)
		r.Aggregator.DeleteSliceUpness(slice.Name)
		return ctrl.Result{}, nil
	}

	if chipCount, err := k8sutils.GetTpuTopologyToChipCount(slice.Spec.Topology); err != nil {
		lg.Error(err, "failed to convert TPU topology to chip count", "slice", slice.Name)
	} else {
		attrs.TPUChipCount = int32(chipCount)
	}

	isReady := false
	for _, cond := range slice.Status.Conditions {
		if cond.Type == slicev1beta1.SliceStateConditionType {
			if cond.Status == metav1.ConditionTrue {
				isReady = true
			}
			break
		}
	}

	// Fetch owner status
	ownerExists, ownerTerminal, err := r.getOwnerStatus(ctx, ownerKind, attrs.SliceOwnerName, attrs.SliceOwnerNamespace)
	if err != nil {
		lg.Error(err, "Error looking up slice owner", "sliceOwner", attrs.SliceOwnerName, "namespace", attrs.SliceOwnerNamespace)
		return ctrl.Result{}, err
	}

	isTerminating := !slice.DeletionTimestamp.IsZero()
	expectedDown := false

	prevUpness, hasPrev := r.Aggregator.GetSliceUpness(slice.Name)
	previousUp := hasPrev && prevUpness.Up(r.Aggregator.UnknownCountThreshold)

	// Determine if the current state transition constitutes "Expected Downtime" (e.g. planned deletion)
	if isTerminating {
		// Case: The slice is terminating. If the owner is missing/terminal, this is an expected down (no interruption)
		expectedDown = !ownerExists || ownerTerminal
	} else {
		// Case: The slice is active and
		// if the slice was previously "Up" but is now suddenly not Ready,
		//  - if owner is gone/terminal -> expectedDown = True
		//  - if owner is still active, this is unplanned -> expectedDown = False
		if previousUp && !isReady {
			expectedDown = !ownerExists || ownerTerminal
		}
	}
	if expectedDown {
		// Case: The downtime is expected (owner terminal or missing), prune from Upness to prevent false interruption
		r.Aggregator.DeleteSliceUpness(slice.Name)
		return ctrl.Result{}, nil
	}

	// slice is expected to be up
	up := records.Upness{
		Attrs:         attrs,
		ExpectedCount: 1,
		ExpectedDown:  expectedDown,
	}

	if isTerminating {
		// Case: The slice is terminating but expectedDown is false.
		up.ReadyCount = 0
	} else {
		// set Upness Ready/Unknown count based on Ready condition
		for _, cond := range slice.Status.Conditions {
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

	// Update the Aggregator with the calculated slice upness.
	// If `isTerminating` is true, it sets `Retain` to true in the Aggregator, so metric will be retained until the owner terminates.
	r.Aggregator.SetSliceUpness(slice.Name, up, isTerminating)

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
		if r.Aggregator.LeaderWorkerSetEnabled {
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
