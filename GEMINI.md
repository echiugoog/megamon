## Project Overview

MegaMon provides metrics related to running JobSets, Slice and Nodepools on top of Kubernetes. In particular for TPU workloads. It makes assumptions around how TPU node pools are provisioned. In particular it assumes nodepools are provisioned via https://github.com/ai-on-gke/tpu-provisioner

## Development Guidelines

## Architecture
Look at docs/arch.md for overall architecture.

### Languages and Version
 - Use standard library packages when possible
 - Use meaningful variable and function names
 - Add comments for exported functions and types
 - Use `package-level` comments at the top of each package

### Code reviews
 - ensure new code updates or creates unit tests to validate logic
 - check for adequate code coverage (80% ideal, >50% required)
 - ensure comments on existing code are not removed
 - ensure unrelated code changes are not part of change
 - ensure code changes don't introduce unnecessary changes
 - ensure code changes are as simple as possible
 - bias towards readability vs cleverness
 - ensure unrelated code is not removed as part of a change

### Help writing pull requests and commits
Help with writing pull requests with concise subject line and bulleted changes
Do not make any changes to the git repo when asked to help write pull request
and commits

### Committing changes
Commits should be as small as possible, representing distinct changes in code.
Commit messages should be informative but not wordy.

### Building and Testing

Don't change code unrelated to the ask or change
Verify label existance against example resource files, confirm with user if unsure

#### Example files / objects
`example.jobset` has an example of valid labels
`example.nodepool` is YAML from GKE nodepool API, with valid labels
`example.node` is YAML from k8s Node object, with valid labels

Ignore these labels:
 * kwok-simulated-job

### Editing files
* Try and use the same method of editing files, do not mix and match methods
  unless absolutely necessary

### Design

#### Object relationships
 * k8s Nodes belong to a GKE NodePool
   * O(1000) nodes per nodepool
   * O(100) nodepools per cluster
 * O(100) jobsets. Jobs belong to a jobset
   * O(1000) jobs
 * with slicing e.g. SliceEnabled == true
   * O(100) slices
   * 16 nodes per nodepool
   * O(100) node pools
   * O(100) jobsets
     * jobsets target a slice via partition IDs
     * a slice is made up of 1 or more partition IDs
   * node belongs to at most 1 slice and 1 nodepool
   * a slice may contain 1 or more nodepools
   * a slice may contain 1 or more subblocks
   * a block contains 1 or more sub blocks
   * 1 subblock is 16 nodes
   * a nodepool belongs to a distinct subblock
   * O(100) subblocks per block
   * Block and SubBlock are only present when SliceEnabled == true

#### Slice CRD
* valid states are ACTIVE, ACTIVE_DEGRADED, ACTIVATING, INCOMPLETE, DEACTIVATING, FAILED or UNKNOWN

#### Kubernetes manifests, labels
 - If unsure about whether a label or value exists, ask user for an example
 - validate against official documentation on label and values if possible

#### Build commands
 - `make` - build binary

#### Testing
- `make test-unit` - run all unit tests
- `make test-integration-parallel` - run integration tests in parallel (ideal if available)
- `make test-integration-verbose` - run integration tests with more details,
  only if needed
- examine Makefile to run ginkgo directly and focus (e.g. "--focus" flag) on specific tests if needed
- Place test files alongside the code they test
- Write table-driven tests when testing multiple scenarios
- Where possible, ensure integration tests can be run in parallel
- Perform a self code review using code review guidelines and remediate
- Perform mutation testing to identify fragile tests
- Perform security checks `go run github.com/securego/gosec/v2/cmd/gosec@latest ./...` and remediate

#### Mutation testing
- perform mutation testing (change the test slight and re-test) on any new tests, example of some changes
 * use mutt-ng branch of go-mutesting to perform mutation tests: https://github.com/echiugoog/go-mutesting/tree/mutt-ng
   * generate JSON and HTML report of testing using go-mutesting config file

#### Code Quality Checks
 - run `go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -test ./...` to check for modern Go idioms
 - use `go fmt` on changed/new files to format

### Error handling
- return errors explicitly; don't panic except for unrecoverable errors
- Provide meaningful error messages that help users understand what went wrong
- log errors appropriately

### Logging
- Include relevant context in log messages
- Use `log.V(3).Info("DEBUG", ...` for DEBUG logging statements

### Performance Considerations
- Use efficient data structures and struct ordering
- Consider memory usage when processing large datasets
- Follow struct field alignment guidelines from https://goperf.dev/01-common-patterns/fields-alignment/
- Preallocate memory where possible (from https://goperf.dev/01-common-patterns/mem-prealloc/)
- Use zero-copy techniques (from https://goperf.dev/01-common-patterns/zero-copy/)

### Dependencies
- Keep dependencies minimal and well-maintained
- Update dependencies carefully and test thoroughly
- Document any new dependencies and their purpose

### Documentation
- Update README.md for user-facing changes
- Use comments for complex blocks of logic or where not immediate obvious what
  is happening


