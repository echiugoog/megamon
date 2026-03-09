# MegaMon Architecture

## Aggregator Component

The `Aggregator` is the core engine of MegaMon, responsible for discovering Kubernetes resources, tracking their uptime history, and producing comprehensive reports for exporters (e.g., Prometheus). 

To ensure clear separation of concerns, the Aggregator is designed as a coordinator that delegates work to three specialized, interface-backed sub-components:

1. **Resource Poller (`internal/aggregator/poller`)**
   - **Interface:** `ResourcePoller`
   - **Role:** Acts as the "Reader" of the current cluster state. It interacts with the Kubernetes and GKE APIs to list JobSets, Nodes, LeaderWorkerSets, and NodePools. It evaluates these resources to determine their current, raw operational status (e.g., how many replicas are ready vs expected).
   - **Boundary:** It has no knowledge of historical events or storage. It populates the raw, current "upness" states directly into the `records.Report` object passed to it.

2. **Event Reconciler (`internal/aggregator/events`)**
   - **Interface:** `EventReconciler`
   - **Role:** Acts as the "State Manager". It fetches historical state events from Google Cloud Storage (GCS), compares this history against the *current* upness provided by the Poller, determines state transitions (e.g., interruptions, recoveries), and persists the updated event log back to GCS. It also instruments GCS latency metrics.
   - **Boundary:** It relies entirely on the inputs provided to it. It does not query Kubernetes directly; it only reconciles data sets.

3. **Summary Producer (`internal/aggregator/report`)**
   - **Interface:** `SummaryProducer`
   - **Role:** Acts as the "Transformer" and orchestrator of historical data. It fetches the latest historical event records from Google Cloud Storage (GCS) and combines them with the raw upness attributes from the Poller. It then calculates time-based metrics (such as total up-time, down-time, MTTR, etc.) to produce the final `UpnessSummaryWithAttrs` for all resource types.
   - **Boundary:** It relies on an `EventGetter` interface to fetch historical events, keeping the orchestration of summary generation centralized and clean.

## Reconcilers
While most resources are polled periodically, MegaMon uses native Kubernetes reconcilers for specific resources to react to state changes in near real-time and reduce API server load.

1. **Slice Reconciler (`internal/controller/slice_reconciler.go`)**
   - **Role:** Watches for Create/Update/Delete events on `Slice` custom resources.
   - **Independence:** This is an independent worker. Whenever a slice event occurs, it lists all slices (using a direct API reader to bypass cache staleness) and performs a full reconciliation against the event history in GCS (`slices.json`).
   - **Integration:** The `Aggregator` relies on the `SliceProvider` interface (which this reconciler implements) to synchronously pull the latest in-memory slice state during its aggregation loop. The primary source of truth for historical reporting remains the GCS event store.

### Workflow
The `Aggregator.Aggregate()` method coordinates these components in a linear workflow:
1. Call the **Poller** to get the current state for JobSets, Nodes, LeaderWorkerSets, and NodePools.
2. For those resource types, call the **Reconciler** to update historical events in GCS based on the current state.
3. Delegate to the **Summary Producer** to fetch the latest historical events from GCS for all resource types (including Slices) and generate the final time-based metrics.
4. Update the thread-safe global `Report` object for consumption by Exporters.
