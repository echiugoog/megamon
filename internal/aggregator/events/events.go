// Package events is responsible for managing historical event records.
// It acts as the "State Manager" for the Aggregator, fetching past state from GCS, reconciling it against current upness, and persisting the updated event log.
package events

import (
	"context"
	"fmt"
	"strings"
	"time"

	"example.com/megamon/internal/records"
)

type EventStore interface {
	Get(ctx context.Context, filename string) (map[string]records.EventRecords, error)
	Put(ctx context.Context, filename string, recs map[string]records.EventRecords) error
}

type Reconciler struct {
	Store                 EventStore
	UnknownCountThreshold float64
}

func NewReconciler(store EventStore, threshold float64) *Reconciler {
	return &Reconciler{
		Store:                 store,
		UnknownCountThreshold: threshold,
	}
}

// Reconcile compares current upness values with historical GCS events. If changes are detected, it updates GCS.
func (r *Reconciler) Reconcile(ctx context.Context, now time.Time, filename string, ups map[string]records.Upness) (map[string]records.EventRecords, error) {
	recs, err := r.Store.Get(ctx, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to get %q: %w", filename, err)
	}

	if changed := records.ReconcileEvents(ctx, now, ups, recs, r.UnknownCountThreshold); changed {
		if errPut := r.Store.Put(ctx, filename, recs); errPut != nil {
			return nil, fmt.Errorf("failed to put %q: %w", filename, errPut)
		}
	}

	return recs, nil
}

// SyncSlices evaluates slices marked for deletion, removes them if their owner is inactive,
// and populates the remaining active slices into the report.
func (r *Reconciler) SyncSlices(sliceProvider records.SliceProvider, report *records.Report) {
	if sliceProvider == nil {
		return
	}

	kindOwnerKey := func(kind, ns, name string) string {
		return fmt.Sprintf("%s/%s/%s", strings.ToLower(kind), ns, name)
	}

	activeSliceOwners := map[string]bool{}
	for _, up := range report.JobSetsUp {
		if up.Status != "Completed" {
			activeSliceOwners[kindOwnerKey("jobset", up.Attrs.JobSetNamespace, up.Attrs.JobSetName)] = true
		}
	}
	for _, up := range report.LeaderWorkerSetsUp {
		activeSliceOwners[kindOwnerKey("leaderworkerset", up.Attrs.JobSetNamespace, up.Attrs.JobSetName)] = true
	}

	slicesUp := sliceProvider.GetSlices()
	for name, state := range slicesUp {
		if state.Deleted {
			kind := state.Upness.Attrs.SliceOwnerKind
			ns := state.Upness.Attrs.SliceOwnerNamespace
			ownerName := state.Upness.Attrs.SliceOwnerName

			if !activeSliceOwners[kindOwnerKey(kind, ns, ownerName)] {
				sliceProvider.DeleteSlice(name)
				continue
			}
		}
		report.SlicesUp[name] = state.Upness
	}
}
