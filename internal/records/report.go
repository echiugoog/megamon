package records

import "math"

func NewReport() Report {
	return Report{
		JobSetsUp:                   make(map[string]Upness),
		JobSetsUpSummaries:          make(map[string]UpnessSummaryWithAttrs),
		JobSetNodesUp:               make(map[string]Upness),
		JobSetNodesUpSummaries:      make(map[string]UpnessSummaryWithAttrs),
		LeaderWorkerSetsUp:          make(map[string]Upness),
		LeaderWorkerSetsUpSummaries: make(map[string]UpnessSummaryWithAttrs),
		NodePoolsUp:                 make(map[string]Upness),
		NodePoolsUpSummaries:        make(map[string]UpnessSummaryWithAttrs),
		SlicesUp:                    make(map[string]Upness),
		SlicesUpSummaries:           make(map[string]UpnessSummaryWithAttrs),
	}
}

func (r Report) Clone() Report {
	cp := NewReport()
	for k, v := range r.JobSetsUp {
		cp.JobSetsUp[k] = v
	}
	for k, v := range r.JobSetsUpSummaries {
		cp.JobSetsUpSummaries[k] = v
	}
	for k, v := range r.JobSetNodesUp {
		cp.JobSetNodesUp[k] = v
	}
	for k, v := range r.JobSetNodesUpSummaries {
		cp.JobSetNodesUpSummaries[k] = v
	}
	for k, v := range r.LeaderWorkerSetsUp {
		cp.LeaderWorkerSetsUp[k] = v
	}
	for k, v := range r.LeaderWorkerSetsUpSummaries {
		cp.LeaderWorkerSetsUpSummaries[k] = v
	}
	for k, v := range r.NodePoolsUp {
		cp.NodePoolsUp[k] = v
	}
	for k, v := range r.NodePoolsUpSummaries {
		cp.NodePoolsUpSummaries[k] = v
	}
	for k, v := range r.SlicesUp {
		cp.SlicesUp[k] = v
	}
	for k, v := range r.SlicesUpSummaries {
		cp.SlicesUpSummaries[k] = v
	}
	if r.NodePoolScheduling != nil {
		cp.NodePoolScheduling = make(map[string]ScheduledJob, len(r.NodePoolScheduling))
		for k, v := range r.NodePoolScheduling {
			cp.NodePoolScheduling[k] = v
		}
	}
	return cp
}

type Report struct {
	JobSetsUp                   map[string]Upness                 `json:"jobSetsUp"`
	JobSetsUpSummaries          map[string]UpnessSummaryWithAttrs `json:"jobSetsUpSummaries"`
	JobSetNodesUp               map[string]Upness                 `json:"jobSetNodesUp"`
	JobSetNodesUpSummaries      map[string]UpnessSummaryWithAttrs `json:"jobSetNodesUpSummaries"`
	LeaderWorkerSetsUp          map[string]Upness                 `json:"leaderWorkerSetsUp"`
	LeaderWorkerSetsUpSummaries map[string]UpnessSummaryWithAttrs `json:"leaderWorkerSetsUpSummaries"`
	NodePoolsUp                 map[string]Upness                 `json:"nodePoolsUp"`
	NodePoolsUpSummaries        map[string]UpnessSummaryWithAttrs `json:"nodePoolsUpSummaries"`
	SlicesUp                    map[string]Upness                 `json:"slicesUp"`
	SlicesUpSummaries           map[string]UpnessSummaryWithAttrs `json:"slicesUpSummaries"`
	NodePoolScheduling          map[string]ScheduledJob           `json:"nodePoolScheduling"`
}

type Attrs struct {
	JobSetName      string `json:"jobsetName"`
	JobSetNamespace string `json:"jobsetNamespace"`
	JobSetUID       string `json:"jobsetUID"`

	LWSName      string `json:"lwsName"`
	LWSNamespace string `json:"lwsNamespace"`
	LWSUID       string `json:"lwsUID"`

	TPUTopology    string `json:"tpuTopology"`
	TPUAccelerator string `json:"tpuAccelerator"`
	TPUChipCount   int32  `json:"tpuChipCount"`
	Spot           bool   `json:"spot"`

	NodePoolName string `json:"nodePoolName"`

	SliceName           string `json:"sliceName"`
	SliceUID            string `json:"sliceUID"`
	SliceOwnerKind      string `json:"sliceOwnerKind"`
	SliceOwnerName      string `json:"sliceOwnerName"`
	SliceOwnerNamespace string `json:"sliceOwnerNamespace"`
}

type Upness struct {
	ReadyCount    int32  `json:"readyCount"`
	ExpectedCount int32  `json:"expectedCount"`
	UnknownCount  int32  `json:"unknownCount"`
	Status        string `json:"status"`
	ExpectedDown  bool   `json:"expectedDown"`
	Attrs
}

// Up determines if a component is considered "up" based on its ready, expected, and unknown counts.
// It allows for a configurable threshold (unknownThreshold) of unknown instances to still be considered "up".
func (up Upness) Up(unknownThreshold float64) bool {
	maxUnknown := int32(math.RoundToEven(float64(up.ExpectedCount) * unknownThreshold))
	if up.UnknownCount > maxUnknown {
		return false
	}
	if up.ReadyCount+up.UnknownCount == up.ExpectedCount {
		return true
	}
	return false
}

type ScheduledJob struct {
	JobName    string `json:"jobName"`
	JobSetName string `json:"jobsetName"`
}

type SliceState struct {
	Upness  Upness
	Deleted bool
}

type SliceProvider interface {
	GetSlices() map[string]SliceState
	DeleteSlice(name string)
}
