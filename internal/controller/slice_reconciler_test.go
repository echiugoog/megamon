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
	"example.com/megamon/internal/aggregator/poller"
	"example.com/megamon/internal/k8sutils"
	"example.com/megamon/internal/metrics"
	"example.com/megamon/internal/records"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

// aggregator should process updates very quickly for slice, no need to wait for aggregation interval
func waitForSliceUpdate(r *SliceReconciler, sliceName string, expectExists bool, expectDown bool) (records.Upness, bool, bool) {
	var currentUpness records.Upness
	var ok bool
	success := false
	// wait up to 1 second
	for range 20 {
		time.Sleep(50 * time.Millisecond)
		state, ok2 := r.GetSlices()[sliceName]
		currentUpness = state.Upness
		ok = ok2
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
	metrics.Init(context.Background(), &dummyReporter{}, 1*time.Second, 0, false)
	t.Parallel()
	scheme := setupScheme()
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	agg := &aggregator.Aggregator{SliceEnabled: true, AggregationInterval: 1 * time.Hour, ResourcePoller: poller.NewPoller(client, nil, true, false), EventStore: &dummyReconciler{}, EventReconciler: &dummyReconciler{}, SummaryProducer: &dummyProducer{}}
	agg.Init()
	loopCtx, cancelLoop := context.WithCancel(context.Background())
	defer cancelLoop()
	go agg.Start(loopCtx)

	r := &SliceReconciler{
		Client:          client,
		Scheme:          scheme,
		EventReconciler: agg.EventReconciler,
	}
	agg.SliceProvider = r

	sliceName := "test-slice"
	sliceNamespace := "default"

	// Inject previous state
	r.SetAllSliceUpness(map[string]records.SliceState{
		sliceName: {
			Upness: records.Upness{
				Attrs: records.Attrs{
					SliceOwnerKind: "JobSet",
				},
			},
			Deleted: false,
		},
	})

	// Wait for aggregator to process the update
	waitForSliceUpdate(r, sliceName, true, false)

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
	_, _, deleted := waitForSliceUpdate(r, sliceName, false, false)

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
	metrics.Init(context.Background(), &dummyReporter{}, 1*time.Second, 0, false)
	t.Parallel()
	scheme := setupScheme()
	ctx := context.Background()

	newSlice := func(name, ownerName string, isReady bool, isTerminating bool) *slicev1beta1.Slice {
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
		if isTerminating {
			s.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			s.Finalizers = []string{"dummy.finalizer"}
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
			name:                 "DeletionTimestamp present: Slice Terminating, Up, Owner exists, Owner terminal -> expectedDown=TRUE",
			slice:                newSlice("slice1", "js1", true, true),
			sliceDeleted:         false,
			owner:                newOwner("js1", true),
			previousUp:           true,
			expectedExpectedDown: true,
		},
		{
			name:                 "DeletionTimestamp present: Slice Terminating, Up, Owner exists, Owner NOT terminal -> expectedDown=FALSE",
			slice:                newSlice("slice2", "js2", true, true),
			sliceDeleted:         false,
			owner:                newOwner("js2", false),
			previousUp:           true,
			expectedExpectedDown: false,
		},
		{
			name:                 "DeletionTimestamp present: Slice Terminating, Up, Owner NOT exists -> expectedDown=TRUE",
			slice:                newSlice("slice3", "missing-js", true, true),
			sliceDeleted:         false,
			previousUp:           true,
			expectedExpectedDown: true,
		},
		{
			name:                 "Slice gone: Slice Active, PrevUp, CurrentDown, Owner exists, Owner terminal -> expectedDown=TRUE",
			slice:                newSlice("slice4", "js4", false, false),
			sliceDeleted:         false,
			owner:                newOwner("js4", true),
			previousUp:           true,
			expectedExpectedDown: true,
		},
		{
			name:                 "Slice gone: Slice Active, PrevUp, CurrentDown, Owner exists, Owner NOT terminal -> expectedDown=FALSE",
			slice:                newSlice("slice5", "js5", false, false),
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

			agg := &aggregator.Aggregator{SliceEnabled: true, AggregationInterval: 1 * time.Hour, ResourcePoller: poller.NewPoller(cl, nil, true, false), EventStore: &dummyReconciler{}, EventReconciler: &dummyReconciler{}, SummaryProducer: &dummyProducer{}}
			agg.Init()
			loopCtx, cancelLoop := context.WithCancel(context.Background())
			defer cancelLoop()
			go agg.Start(loopCtx)

			r := &SliceReconciler{
				Client:          cl,
				Scheme:          scheme,
				EventReconciler: agg.EventReconciler,
			}
			agg.SliceProvider = r

			if tt.previousUp {
				attrs := records.Attrs{
					SliceName:           tt.slice.Name,
					SliceUID:            string(tt.slice.UID),
					SliceOwnerKind:      tt.slice.Labels[k8sutils.LabelTPUProvisionerOwnerKind],
					SliceOwnerName:      tt.slice.Labels[k8sutils.LabelTPUProvisionerOwnerName],
					SliceOwnerNamespace: tt.slice.Labels[k8sutils.LabelTPUProvisionerOwnerNamespace],
				}
				r.SetAllSliceUpness(map[string]records.SliceState{
					tt.slice.Name: {
						Upness: records.Upness{
							ReadyCount:    1,
							ExpectedCount: 1,
							Attrs:         attrs,
						},
						Deleted: false,
					},
				})
				// Wait for aggregator to process the update
				waitForSliceUpdate(r, tt.slice.Name, true, false)
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
			// We expect the slice to exist in the map regardless of expectedDown,
			// unless the slice itself is completely deleted from the apiserver.
			expectExists := !tt.sliceDeleted
			currentUpness, ok, success := waitForSliceUpdate(r, tt.slice.Name, expectExists, tt.expectedExpectedDown)

			if !success {
				if !expectExists {
					t.Errorf("Expected slice to be pruned from the map completely, got ok=%v, upness=%+v", ok, currentUpness)
				} else {
					t.Errorf("Expected slice to exist with expectedDown=%v, got ok=%v, upness=%+v", tt.expectedExpectedDown, ok, currentUpness)
				}
			}
		})
	}
}

func TestSliceReconciler_Reconcile_Deletion_WithInvalidOwnerKind(t *testing.T) {
	metrics.Init(context.Background(), &dummyReporter{}, 1*time.Second, 0, false)
	t.Parallel()
	scheme := setupScheme()
	sliceName := "test-slice-invalid-owner"
	sliceNamespace := "default"

	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	agg := &aggregator.Aggregator{SliceEnabled: true, AggregationInterval: 1 * time.Hour, ResourcePoller: poller.NewPoller(cl, nil, true, false), EventStore: &dummyReconciler{}, EventReconciler: &dummyReconciler{}, SummaryProducer: &dummyProducer{}}
	agg.Init()

	loopCtx, cancelLoop := context.WithCancel(context.Background())
	defer cancelLoop()
	go agg.Start(loopCtx)

	r := &SliceReconciler{
		Client:          cl,
		Scheme:          scheme,
		EventReconciler: agg.EventReconciler,
	}
	agg.SliceProvider = r

	// Inject previous state via public SetSliceUpness
	r.SetAllSliceUpness(map[string]records.SliceState{
		sliceName: {
			Upness: records.Upness{
				Attrs: records.Attrs{
					SliceOwnerKind: "UnknownKind",
				},
			},
			Deleted: false,
		},
	})

	// Wait for aggregator to process the update
	waitForSliceUpdate(r, sliceName, true, false)

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
	_, _, success := waitForSliceUpdate(r, sliceName, false, false)
	if !success {
		t.Error("Expected slice to be pruned because owner kind is invalid")
	}
}

type dummyReporter struct{}

func (d *dummyReporter) Report() records.Report {
	return records.Report{}
}

type dummyPoller struct{}

func (d *dummyPoller) PollResources(ctx context.Context, report *records.Report) error { return nil }

type dummyReconciler struct{}

func (d *dummyReconciler) Get(ctx context.Context, filename string) (map[string]records.EventRecords, error) {
	return nil, nil
}

func (d *dummyReconciler) Reconcile(ctx context.Context, now time.Time, filename string, ups map[string]records.Upness) (map[string]records.EventRecords, error) {
	return nil, nil
}

type dummyProducer struct{}

func (d *dummyProducer) GenerateSummaries(ctx context.Context, now time.Time, store aggregator.EventStore, sliceEnabled, lwsEnabled bool, report *records.Report) error {
	return nil
}
func (d *dummyReporter) ReportReady() bool { return true }

func (d *dummyReconciler) SyncSlices(sliceProvider records.SliceProvider, report *records.Report) {
}

func (d *dummyReconciler) Put(ctx context.Context, filename string, recs map[string]records.EventRecords) error {
	return nil
}
