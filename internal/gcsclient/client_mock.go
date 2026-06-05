package gcsclient

import (
	"context"
	"sync"

	"example.com/megamon/internal/records"
)

type mockGCSClient struct {
	mu      sync.RWMutex
	records map[string]map[string]records.EventRecords
}

func (m *mockGCSClient) GetRecords(ctx context.Context, bucket, path string) (map[string]records.EventRecords, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.records[path]
	if !ok {
		return map[string]records.EventRecords{}, nil
	}
	return cloneEventRecordsMap(rec), nil
}

func (m *mockGCSClient) PutRecords(ctx context.Context, bucket, path string, recs map[string]records.EventRecords) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[path] = cloneEventRecordsMap(recs)
	return nil
}

func CreateStubGCSClient() *mockGCSClient {
	return &mockGCSClient{
		records: make(map[string]map[string]records.EventRecords),
	}
}

func cloneEventRecords(r records.EventRecords) records.EventRecords {
	if r.UpEvents == nil {
		return records.EventRecords{}
	}
	upEventsCp := make([]records.UpEvent, len(r.UpEvents))
	copy(upEventsCp, r.UpEvents)
	return records.EventRecords{
		UpEvents: upEventsCp,
	}
}

func cloneEventRecordsMap(original map[string]records.EventRecords) map[string]records.EventRecords {
	if original == nil {
		return nil
	}
	cp := make(map[string]records.EventRecords, len(original))
	for k, v := range original {
		cp[k] = cloneEventRecords(v)
	}
	return cp
}
