package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	slicev1beta1 "example.com/megamon/copied-slice-api/v1beta1"
	"example.com/megamon/internal/aggregator"
	"example.com/megamon/internal/k8sutils"
	"example.com/megamon/internal/records"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

// aggregator should process updates very quickly for slice, no need to wait for aggregation interval
func waitForSliceUpdate(agg *aggregator.Aggregator, sliceName string, expectExists bool, expectDown bool) (records.Upness, bool, bool) {
	var currentUpness records.Upness
	var ok bool
	success := false
	// wait up to 1 second
	for range 20 {
		time.Sleep(50 * time.Millisecond)
		currentUpness, ok = agg.GetSliceUpness(sliceName)
		if !expectExists {
			if !ok {
				success = true
				break
			}
		} else {
			if ok && currentUpness.ExpectedDown == expectDown {
				success = true
				break
			}
		}
	}
	return currentUpness, ok, success
}

func setupScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = slicev1beta1.AddToScheme(scheme)
	_ = jobset.AddToScheme(scheme)
	_ = lws.AddToScheme(scheme)
	return scheme
}

func TestSliceReconciler_Reconcile_Deletion(t *testing.T) {
	t.Parallel()
	scheme := setupScheme()
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	aggregationInterval := 1 * time.Second
	agg := aggregator.NewAggregator(client, nil, nil, aggregator.Config{
		Interval:               aggregationInterval,
		SliceEnabled:           true,
		LeaderWorkerSetEnabled: false,
	})
	loopCtx, cancelLoop := context.WithCancel(context.Background())
	defer cancelLoop()
	go agg.Start(loopCtx)

	sliceName := "test-slice"
	sliceNamespace := "default"

	// Inject previous state
	agg.SetSliceUpness(sliceName, records.Upness{
		Attrs: records.Attrs{
			SliceOwnerKind: "JobSet",
		},
	}, false)

	// Wait for aggregator to process the update
	waitForSliceUpdate(agg, sliceName, true, false)

	r := &SliceReconciler{
		Client:     client,
		Scheme:     scheme,
		Aggregator: agg,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      sliceName,
			Namespace: sliceNamespace,
		},
	}

	ctx := context.Background()
	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Verify deletion update processed (pruned because owner missing)
	_, _, deleted := waitForSliceUpdate(agg, sliceName, false, false)

	if !deleted {
		t.Error("Expected slice to be pruned because owner is missing")
	}
}

/*
| DeletionTimestamp | Previous Up/Down | Owner exists | Owner terminal | expectedDown | interruption/recovery |
|Present |Up      |TRUE    |TRUE    |TRUE       | None            |
|Present |Up      |TRUE    |FALSE   |FALSE      | +1 interruption |
|Present |Up      |FALSE   |        |TRUE       | None            |
|Present |Down    |TRUE    |FALSE   |FALSE      | None            |
|Present |Down    |TRUE    |TRUE    |TRUE       | None            |
|Present |Down    |FALSE   |        |TRUE       | None            |

|DeletionTimestamp| Previous Up/Down | Current Up? | Owner exists | Owner Terminal | expectedDown | Interruption/ Recovery |
|NotPresent      |Up      |FALSE   |TRUE    |TRUE    |TRUE    |None             |
|NotPresent      |Up      |FALSE   |TRUE    |FALSE   |FALSE   | +1 interruption |
|NotPresent      |Up      |FALSE   |FALSE   |        |TRUE    | None            |
|NotPresent      |Up      |TRUE    |        |        |        | None            |
|NotPresent      |Down    |TRUE    |TRUE    |TRUE    |        | +1 recovery     |
|NotPresent      |Down    |TRUE    |TRUE    |FALSE   |        | +1 recovery     |
|NotPresent      |Down    |TRUE    |FALSE   |        |        | +1 recovery     |
|NotPresent      |Down    |Down    |        |        |        |None             |
*/
func TestSliceReconciler_Logic(t *testing.T) {
	t.Parallel()
	scheme := setupScheme()
	ctx := context.Background()
	aggregationInterval := 1 * time.Second

	newSlice := func(name, ownerName string, isReady bool) *slicev1beta1.Slice {
		s := &slicev1beta1.Slice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels: map[string]string{
					k8sutils.LabelTPUProvisionerOwnerKind:      "JobSet",
					k8sutils.LabelTPUProvisionerOwnerName:      ownerName,
					k8sutils.LabelTPUProvisionerOwnerNamespace: "default",
				},
			},
			Status: slicev1beta1.SliceStatus{
				Conditions: []metav1.Condition{{Type: slicev1beta1.SliceStateConditionType}},
			},
		}
		if isReady {
			s.Status.Conditions[0].Status = metav1.ConditionTrue
		} else {
			s.Status.Conditions[0].Status = metav1.ConditionFalse
		}
		return s
	}

	newOwner := func(name string, isTerminal bool) *jobset.JobSet {
		js := &jobset.JobSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		}
		if isTerminal {
			js.Status.Conditions = []metav1.Condition{{Type: string(jobset.JobSetCompleted), Status: metav1.ConditionTrue}}
		}
		return js
	}

	tests := []struct {
		name                 string
		slice                *slicev1beta1.Slice
		sliceDeleted         bool
		owner                runtime.Object
		previousUp           bool
		expectedExpectedDown bool
	}{
		{
			name:                 "DeletionTimestamp present: Slice Deleted, Up, Owner exists, Owner terminal -> expectedDown=TRUE",
			slice:                newSlice("slice1", "js1", true),
			sliceDeleted:         true,
			owner:                newOwner("js1", true),
			previousUp:           true,
			expectedExpectedDown: true,
		},
		{
			name:                 "DeletionTimestamp present: Slice Deleted, Up, Owner exists, Owner NOT terminal -> expectedDown=FALSE",
			slice:                newSlice("slice2", "js2", true),
			sliceDeleted:         true,
			owner:                newOwner("js2", false),
			previousUp:           true,
			expectedExpectedDown: false,
		},
		{
			name:                 "DeletionTimestamp present: Slice Deleted, Up, Owner NOT exists -> expectedDown=TRUE",
			slice:                newSlice("slice3", "missing-js", true),
			sliceDeleted:         true,
			previousUp:           true,
			expectedExpectedDown: true,
		},
		{
			name:                 "Slice gone: Slice Active, PrevUp, CurrentDown, Owner exists, Owner terminal -> expectedDown=TRUE",
			slice:                newSlice("slice4", "js4", false),
			sliceDeleted:         false,
			owner:                newOwner("js4", true),
			previousUp:           true,
			expectedExpectedDown: true,
		},
		{
			name:                 "Slice gone: Slice Active, PrevUp, CurrentDown, Owner exists, Owner NOT terminal -> expectedDown=FALSE",
			slice:                newSlice("slice5", "js5", false),
			sliceDeleted:         false,
			owner:                newOwner("js5", false),
			previousUp:           true,
			expectedExpectedDown: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objs []runtime.Object
			if !tt.sliceDeleted {
				objs = append(objs, tt.slice)
			}
			if tt.owner != nil {
				objs = append(objs, tt.owner)
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()

			agg := aggregator.NewAggregator(cl, nil, nil, aggregator.Config{
				Interval:     aggregationInterval,
				SliceEnabled: true,
			})
			loopCtx, cancelLoop := context.WithCancel(context.Background())
			defer cancelLoop()
			go agg.Start(loopCtx)

			if tt.previousUp {
				attrs := records.Attrs{
					SliceName:           tt.slice.Name,
					SliceUID:            string(tt.slice.UID),
					SliceOwnerKind:      tt.slice.Labels[k8sutils.LabelTPUProvisionerOwnerKind],
					SliceOwnerName:      tt.slice.Labels[k8sutils.LabelTPUProvisionerOwnerName],
					SliceOwnerNamespace: tt.slice.Labels[k8sutils.LabelTPUProvisionerOwnerNamespace],
				}
				agg.SetSliceUpness(tt.slice.Name, records.Upness{
					ReadyCount:    1,
					ExpectedCount: 1,
					Attrs:         attrs,
				}, false)
				// Wait for aggregator to process the update
				waitForSliceUpdate(agg, tt.slice.Name, true, false)
			}

			r := &SliceReconciler{
				Client:     cl,
				Scheme:     scheme,
				Aggregator: agg,
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tt.slice.Name,
					Namespace: tt.slice.Namespace,
				},
			}

			_, err := r.Reconcile(ctx, req)
			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}

			// Wait for aggregator to process the reconcile update
			currentUpness, ok, success := waitForSliceUpdate(agg, tt.slice.Name, !tt.expectedExpectedDown, false)

			if !success {
				if tt.expectedExpectedDown {
					t.Errorf("Expected slice to be pruned, but it still exists: %+v", currentUpness)
				} else {
					t.Errorf("Expected slice to exist with expectedDown=false, got ok=%v, upness=%+v", ok, currentUpness)
				}
			}
		})
	}
}

func TestSliceReconciler_Reconcile_Deletion_WithInvalidOwnerKind(t *testing.T) {
	t.Parallel()
	scheme := setupScheme()
	sliceName := "test-slice-invalid-owner"
	sliceNamespace := "default"
	aggregationInterval := 1 * time.Second

	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	agg := aggregator.NewAggregator(cl, nil, nil, aggregator.Config{
		Interval:     aggregationInterval,
		SliceEnabled: true,
	})

	loopCtx, cancelLoop := context.WithCancel(context.Background())
	defer cancelLoop()
	go agg.Start(loopCtx)

	// Inject previous state via public SetSliceUpness
	agg.SetSliceUpness(sliceName, records.Upness{
		Attrs: records.Attrs{
			SliceOwnerKind: "UnknownKind",
		},
	}, false)

	// Wait for aggregator to process the update
	waitForSliceUpdate(agg, sliceName, true, false)

	r := &SliceReconciler{
		Client:     cl,
		Scheme:     scheme,
		Aggregator: agg,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      sliceName,
			Namespace: sliceNamespace,
		},
	}

	ctx := context.Background()
	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Verify the slice state WAS DELETED
	_, _, success := waitForSliceUpdate(agg, sliceName, false, false)
	if !success {
		t.Error("Expected slice to be pruned because owner kind is invalid")
	}
}
