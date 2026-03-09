package aggregator

import (
	"context"
	"time"

	"example.com/megamon/internal/records"
)

type ResourcePoller interface {
	PollResources(ctx context.Context, report *records.Report) error
}

type EventStore interface {
	Get(ctx context.Context, filename string) (map[string]records.EventRecords, error)
	Put(ctx context.Context, filename string, recs map[string]records.EventRecords) error
}

type EventReconciler interface {
	Reconcile(ctx context.Context, now time.Time, filename string, ups map[string]records.Upness) (map[string]records.EventRecords, error)
	SyncSlices(sliceProvider records.SliceProvider, report *records.Report)
}

type SummaryProducer interface {
	GenerateSummaries(ctx context.Context, now time.Time, getter EventStore, sliceEnabled, lwsEnabled bool, report *records.Report) error
}

type Exporter interface {
	Export(ctx context.Context, report records.Report) error
}
