# Solver Budget, Residual Cache, Diagnostics, and Production Fixtures Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make hard-reclaim replacement complete and deterministic for feasible production inputs by separating per-graph memory accounting from whole-search work accounting, caching equivalent residual solves, exposing completion diagnostics, and replaying captured production-shaped fixtures.

**Architecture:** Keep `policy_advisor_partition_solver.go` responsible for one min-cost-flow graph and keep `hard_reclaim_partition.go` responsible for the multi-candidate replacement search. Every residual miss receives a fresh graph budget, while one search budget, cache, and diagnostics object live for exactly one replacement invocation; cached assignments are cloned and every candidate still builds its own validation proof. Tests remain in the `dynamicpolicy` package, with sanitized JSON fixtures under package-local `testdata`.

**Tech Stack:** Go 1.18, `machine.CPUTopology`/`machine.CPUSet`, deterministic min-cost flow, Testify, Go `testing`, JSON test fixtures, Mockey-compatible Go test flags.

## Baseline and Scope

- Approved design: `docs/superpowers/specs/2026-09-23-solver-budget-lifecycle-proof-design.md`.
- Plan baseline: `4577e5da59e949e777d4a1afe710ac377dc1c982`.
- This plan covers only the approved solver work: graph/search budget ownership, residual signature caching, completion diagnostics and typed outcomes, and production fixture replay.
- Container lifecycle proofs, replan disposition, and advisor target liveness are intentionally separate implementation plans.
- Do not increase `partitionAssignmentEdgeBudget`, `partitionFlowOperationBudget`, `hardReclaimReplacementMaxCandidateStates`, or `hardReclaimReplacementMaxTerminalSolves`.
- Preserve deterministic candidate/result ordering and existing `errors.Is` behavior for `errPartitionAssignmentEdgeBudget` and `errPartitionFlowOperationBudget`.

## File Map

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go`: split the mixed budget into one-graph edge accounting and shared flow accounting; expose exact usage to the replacement search.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver_test.go`: RED coverage for per-graph edges, cumulative flow operations, and unchanged standalone solver behavior.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go`: own one search budget/cache/diagnostics object per replacement invocation and distinguish complete infeasibility from budget exhaustion.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache.go`: canonical residual signatures, cache result cloning, and cacheability policy.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache_test.go`: order independence, deep-copy, hit accounting, and non-cacheable error tests.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_diagnostics.go`: invocation-local counters and typed terminal outcomes.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_diagnostics_test.go`: diagnostic accounting and typed outcome tests.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go`: replacement-level RED tests, deterministic result assertions, and production fixture runner.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/testdata/hard_reclaim_replacement/affected-numa2-boundary.json`: sanitized captured NUMA-2 replacement case currently encoded inline by `productionNUMA2ReplacementFixture`.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/testdata/hard_reclaim_replacement/multi-numa-complete-core-deficit.json`: production-shaped multi-NUMA/SMT2 case that exercises multiple residual candidates.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go`: consume the shared fixture loader and prove solver failure is pre-commit.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`: assert revision, checkpoint/WAL files, and advisor post-commit target remain unchanged on a fixture-driven solver failure.

---

### Task 1: Separate Per-Graph Edge Budget from Whole-Search Flow Budget

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go:51-128,160-191`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver_test.go:293-316`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go:613-739,885-896`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go:1139-1184,1283-1318`

- [ ] **Step 1: Add RED tests for graph-local edges and search-global flow**

Append these focused tests to `hard_reclaim_partition_test.go`:

```go
func TestHardReclaimReplacementResetsAssignmentEdgesPerResidualGraph(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(4, 1, 1)
	require.NoError(t, err)
	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	demands := []partitionDemand{
		{
			key: "reclaim", quantity: 1, eligible: numa,
			preferred: machine.NewCPUSet(0), class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "shared", quantity: 1, eligible: numa,
			preferred: machine.NewCPUSet(1), class: advisorBlockClassShared,
		},
	}

	assignments, proof, err := solveHardReclaimWithReplacement(
		demands, numa, topology,
		hardReclaimReplacementOptions{
			maxCandidateStates:          100,
			maxTerminalSolves:           10,
			maxPartitionAssignmentEdges: 4,
			maxPartitionFlowOperations:  partitionFlowOperationBudget,
		},
	)

	require.NoError(t, err)
	require.NotNil(t, proof)
	require.Len(t, assignments, 2)
}

func TestHardReclaimReplacementRejectsOneOversizedResidualGraph(t *testing.T) {
	t.Parallel()

	topology, demands, _, _ := productionNUMA2ReplacementFixture(t)
	assignments, proof, err := solveHardReclaimWithReplacement(
		demands, topology.CPUDetails.CPUs(), topology,
		hardReclaimReplacementOptions{
			maxCandidateStates:          100_000,
			maxTerminalSolves:           4096,
			maxPartitionAssignmentEdges: 1,
			maxPartitionFlowOperations:  partitionFlowOperationBudget,
		},
	)

	require.Nil(t, assignments)
	require.Nil(t, proof)
	require.ErrorIs(t, err, errPartitionAssignmentEdgeBudget)
}

func TestHardReclaimReplacementSharesFlowOperationsAcrossResidualGraphs(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopologyWithoutSMT(4, 1, 1)
	require.NoError(t, err)
	numa := topology.CPUDetails.CPUsInNUMANodes(0)
	demands := []partitionDemand{
		{key: "reclaim", quantity: 1, eligible: numa, class: advisorBlockClassMandatoryReclaim},
		{key: "shared", quantity: 1, eligible: numa, class: advisorBlockClassShared},
	}

	assignments, proof, err := solveHardReclaimWithReplacement(
		demands, numa, topology,
		hardReclaimReplacementOptions{
			maxCandidateStates:          100,
			maxTerminalSolves:           10,
			maxPartitionAssignmentEdges: partitionAssignmentEdgeBudget,
			maxPartitionFlowOperations:  1,
		},
	)

	require.Nil(t, assignments)
	require.Nil(t, proof)
	require.ErrorIs(t, err, errPartitionFlowOperationBudget)
}
```

Replace the old assertion in
`TestHardReclaimReplacementSharesResidualBudgetAcrossGlobalCandidates` that
expects `errPartitionAssignmentEdgeBudget`; rename it to the first test above
so the old behavior is explicitly the RED condition.

- [ ] **Step 2: Run the budget RED tests**

Run:

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestHardReclaimReplacementResetsAssignmentEdgesPerResidualGraph|TestHardReclaimReplacementRejectsOneOversizedResidualGraph|TestHardReclaimReplacementSharesFlowOperationsAcrossResidualGraphs)$' \
  -count=1 -v
```

Expected: FAIL. The reset test returns a wrapped
`errPartitionAssignmentEdgeBudget` because the current
`partitionSolverBudget.assignmentEdges` is reused by every terminal; the
single-graph and flow-budget tests continue to fail closed.

- [ ] **Step 3: Split the solver budget types and signatures**

Replace `partitionSolverBudget` in
`policy_advisor_partition_solver.go` with:

```go
type partitionGraphBudget struct {
	maxAssignmentEdges int
	assignmentEdges    int
}

type partitionSearchBudget struct {
	maxFlowOperations int
	flowOperations    int
}

func defaultPartitionGraphBudget() partitionGraphBudget {
	return partitionGraphBudget{maxAssignmentEdges: partitionAssignmentEdgeBudget}
}

func defaultPartitionSearchBudget() partitionSearchBudget {
	return partitionSearchBudget{maxFlowOperations: partitionFlowOperationBudget}
}
```

Keep the simple entry point, but make the ownership visible:

```go
func solveDisjointPartitions(
	demands []partitionDemand,
	topology *machine.CPUTopology,
) (map[string]machine.CPUSet, error) {
	graphBudget := defaultPartitionGraphBudget()
	searchBudget := defaultPartitionSearchBudget()
	return solveDisjointPartitionsWithBudgets(
		demands, topology, &graphBudget, &searchBudget)
}

func solveDisjointPartitionsWithBudgets(
	demands []partitionDemand,
	topology *machine.CPUTopology,
	graphBudget *partitionGraphBudget,
	searchBudget *partitionSearchBudget,
) (map[string]machine.CPUSet, error) {
	if graphBudget == nil || graphBudget.maxAssignmentEdges <= 0 {
		return nil, errPartitionAssignmentEdgeBudget
	}
	if searchBudget == nil || searchBudget.maxFlowOperations <= 0 {
		return nil, errPartitionFlowOperationBudget
	}
	sortedDemands, cpus, total, err := validatePartitionDemands(demands, topology)
	if err != nil {
		return nil, err
	}
	return solveValidatedDisjointPartitions(
		sortedDemands, cpus, total, topology, graphBudget, searchBudget)
}
```

Move the current graph construction and result extraction into
`solveValidatedDisjointPartitions`; its body is unchanged except that graph
construction charges only `graphBudget`:

```go
graphBudget.assignmentEdges++
if graphBudget.assignmentEdges > graphBudget.maxAssignmentEdges {
	return nil, errPartitionAssignmentEdgeBudget
}
```

In min-cost flow, charge only the invocation-wide search owner:

```go
flow, err := partitionMinCostFlowWithUsage(
	graph,
	source,
	sink,
	total,
	&searchBudget.flowOperations,
	searchBudget.maxFlowOperations,
)
```

Delete `partitionSolverBudget`, `defaultPartitionSolverBudget`,
`solveDisjointPartitionsWithBudget`, and
`solveDisjointPartitionsWithSharedBudget`; update solver tests that need custom
limits to construct the two explicit budgets.

- [ ] **Step 4: Give every replacement residual a new graph budget**

Replace `hardReclaimPartitionSolverBudget` with:

```go
func hardReclaimPartitionGraphBudget(
	options hardReclaimReplacementOptions,
) partitionGraphBudget {
	budget := defaultPartitionGraphBudget()
	if options.maxPartitionAssignmentEdges > 0 {
		budget.maxAssignmentEdges = options.maxPartitionAssignmentEdges
	}
	return budget
}

func hardReclaimPartitionSearchBudget(
	options hardReclaimReplacementOptions,
) partitionSearchBudget {
	budget := defaultPartitionSearchBudget()
	if options.maxPartitionFlowOperations > 0 {
		budget.maxFlowOperations = options.maxPartitionFlowOperations
	}
	return budget
}
```

Create one `searchBudget` before the residual-state loop, and a new
`graphBudget` inside each iteration:

```go
searchBudget := hardReclaimPartitionSearchBudget(options)
for _, state := range states {
	residualDemands := hardReclaimResidualDemands(
		demands, available, state.reclaimAfter)
	graphBudget := hardReclaimPartitionGraphBudget(options)
	assignments, solveErr := solveDisjointPartitionsWithBudgets(
		residualDemands, topology, &graphBudget, &searchBudget)
	// Preserve the current fail-closed budget wrapping and candidate validation.
}
```

Use the same ownership in the no-hard-reclaim branch: one fresh graph budget
and the invocation's one search budget.

- [ ] **Step 5: Run focused and package tests**

Run:

```bash
gofmt -w \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestSolveDisjointPartitions|TestHardReclaimReplacement)' \
  -count=1
```

Expected: PASS. `TestSolveDisjointPartitionsFailsFastAtEdgeBudget` still proves
one oversized graph fails, while the replacement reset test now succeeds.

- [ ] **Step 6: Commit the budget ownership change**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go
git commit -m "fix(qrm-cpu): separate graph and search solver budgets"
```

---

### Task 2: Cache Equivalent Residual Solves Per Replacement Invocation

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go:60-63,195-197`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go:613-727`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go:847-863`

- [ ] **Step 1: Add RED signature and cache behavior tests**

Create `hard_reclaim_residual_cache_test.go`:

```go
package dynamicpolicy

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestPartitionResidualSignatureIsOrderIndependentAndComplete(t *testing.T) {
	topology := partitionSolverFixtureTopology()
	base := []partitionDemand{
		{
			key: "a", requestGroupKey: "pod-a/main", quantity: 1,
			requestQuantity: 1.5, eligible: machine.NewCPUSet(0, 1),
			preferred: machine.NewCPUSet(1), class: advisorBlockClassDedicated,
		},
		{
			key: "b", requestGroupKey: "pod-b/main", quantity: 1,
			requestQuantity: 1, eligible: machine.NewCPUSet(2, 3),
			preferred: machine.NewCPUSet(2), class: advisorBlockClassShared,
		},
	}

	first, err := newPartitionResidualSignature(base, topology)
	require.NoError(t, err)
	second, err := newPartitionResidualSignature(
		[]partitionDemand{base[1], base[0]}, topology)
	require.NoError(t, err)
	require.Equal(t, first, second)

	changed := append([]partitionDemand(nil), base...)
	changed[0].requestQuantity = 2
	third, err := newPartitionResidualSignature(changed, topology)
	require.NoError(t, err)
	require.NotEqual(t, first, third)
}

func TestPartitionResidualSolveCacheReturnsDeepCopies(t *testing.T) {
	cache := newPartitionResidualSolveCache()
	signature := partitionResidualSignature("same-input")
	original := map[string]machine.CPUSet{"a": machine.NewCPUSet(0, 1)}

	cache.storeSuccess(signature, original)
	original["a"] = machine.NewCPUSet(7)
	first, ok := cache.lookup(signature)
	require.True(t, ok)
	require.Equal(t, machine.NewCPUSet(0, 1), first.assignments["a"])

	first.assignments["a"] = machine.NewCPUSet(6)
	second, ok := cache.lookup(signature)
	require.True(t, ok)
	require.Equal(t, machine.NewCPUSet(0, 1), second.assignments["a"])
}

func TestPartitionResidualSolveCacheOnlyStoresSemanticResults(t *testing.T) {
	cache := newPartitionResidualSolveCache()
	signature := partitionResidualSignature("same-input")

	for _, err := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		errPartitionAssignmentEdgeBudget,
		errPartitionFlowOperationBudget,
		errors.New("internal"),
	} {
		require.False(t, cache.storeFailure(signature, err), "err=%v", err)
		_, found := cache.lookup(signature)
		require.False(t, found)
	}

	require.True(t, cache.storeFailure(signature, errPartitionNoFeasibleAssignment))
	got, found := cache.lookup(signature)
	require.True(t, found)
	require.ErrorIs(t, got.err, errPartitionNoFeasibleAssignment)
}
```

- [ ] **Step 2: Run the cache RED tests**

Run:

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestPartitionResidual' -count=1 -v
```

Expected: FAIL to compile with undefined
`newPartitionResidualSignature`, `partitionResidualSignature`,
`newPartitionResidualSolveCache`, and
`errPartitionNoFeasibleAssignment`.

- [ ] **Step 3: Introduce a typed semantic-infeasibility sentinel**

In `policy_advisor_partition_solver.go`, add:

```go
var (
	errPartitionAssignmentEdgeBudget = errors.New("partition graph edge budget exceeded")
	errPartitionFlowOperationBudget  = errors.New("partition flow operation budget exceeded")
	errPartitionNoFeasibleAssignment = errors.New("partition demands have no feasible assignment")
)
```

Return `errPartitionNoFeasibleAssignment` from all validated infeasible paths
that currently construct `fmt.Errorf("partition demands have no feasible assignment")`.
Do not convert malformed input, overflow, or budget errors to this sentinel.

- [ ] **Step 4: Implement a canonical, invocation-local cache**

Create `hard_reclaim_residual_cache.go` with the following compile-oriented
implementation:

```go
package dynamicpolicy

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type partitionResidualSignature string

type partitionResidualSignatureDemand struct {
	Key             string `json:"key"`
	RequestGroupKey string `json:"requestGroupKey"`
	Quantity        int    `json:"quantity"`
	RequestQuantity string `json:"requestQuantity"`
	Class           string `json:"class"`
	Eligible        string `json:"eligible"`
	Preferred       string `json:"preferred"`
}

type partitionResidualSignatureCPU struct {
	ID         int `json:"id"`
	NUMANodeID int `json:"numaNodeID"`
	SocketID   int `json:"socketID"`
	CoreID     int `json:"coreID"`
}

type partitionResidualSignaturePayload struct {
	Demands  []partitionResidualSignatureDemand `json:"demands"`
	Topology []partitionResidualSignatureCPU    `json:"topology"`
}

type partitionResidualSolveResult struct {
	assignments map[string]machine.CPUSet
	err         error
}

type partitionResidualSolveCache struct {
	entries map[partitionResidualSignature]partitionResidualSolveResult
}

func newPartitionResidualSolveCache() *partitionResidualSolveCache {
	return &partitionResidualSolveCache{
		entries: make(map[partitionResidualSignature]partitionResidualSolveResult),
	}
}

func newPartitionResidualSignature(
	demands []partitionDemand,
	topology *machine.CPUTopology,
) (partitionResidualSignature, error) {
	sorted := append([]partitionDemand(nil), demands...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].key < sorted[j].key })
	payload := partitionResidualSignaturePayload{
		Demands: make([]partitionResidualSignatureDemand, 0, len(sorted)),
	}
	for _, demand := range sorted {
		payload.Demands = append(payload.Demands, partitionResidualSignatureDemand{
			Key: demand.key, RequestGroupKey: demand.requestGroupKey,
			Quantity: demand.quantity,
			RequestQuantity: strconv.FormatFloat(demand.requestQuantity, 'g', -1, 64),
			Class: string(demand.class),
			Eligible: demand.eligible.String(), Preferred: demand.preferred.String(),
		})
	}
	for _, cpu := range topology.CPUDetails.CPUs().ToSliceInt() {
		info := topology.CPUDetails[cpu]
		payload.Topology = append(payload.Topology, partitionResidualSignatureCPU{
			ID: cpu, NUMANodeID: info.NUMANodeID,
			SocketID: info.SocketID, CoreID: info.CoreID,
		})
	}
	encoded, err := json.Marshal(payload)
	return partitionResidualSignature(encoded), err
}

func clonePartitionAssignments(
	in map[string]machine.CPUSet,
) map[string]machine.CPUSet {
	if in == nil {
		return nil
	}
	out := make(map[string]machine.CPUSet, len(in))
	for key, cpus := range in {
		out[key] = cpus.Clone()
	}
	return out
}

func (c *partitionResidualSolveCache) lookup(
	signature partitionResidualSignature,
) (partitionResidualSolveResult, bool) {
	result, ok := c.entries[signature]
	result.assignments = clonePartitionAssignments(result.assignments)
	return result, ok
}

func (c *partitionResidualSolveCache) storeSuccess(
	signature partitionResidualSignature,
	assignments map[string]machine.CPUSet,
) {
	c.entries[signature] = partitionResidualSolveResult{
		assignments: clonePartitionAssignments(assignments),
	}
}

func (c *partitionResidualSolveCache) storeFailure(
	signature partitionResidualSignature,
	err error,
) bool {
	if !errors.Is(err, errPartitionNoFeasibleAssignment) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	c.entries[signature] = partitionResidualSolveResult{err: errPartitionNoFeasibleAssignment}
	return true
}
```

The signature stores canonical JSON bytes directly instead of a hash, avoiding
collision handling. Sorting demands plus `CPUSet.String()` and sorted topology
CPU IDs makes the key independent of map/input order. The cache has no mutex
because one replacement invocation owns it synchronously.

- [ ] **Step 5: Add a RED integration test proving one miss followed by a deterministic hit**

Add a small production helper in `hard_reclaim_residual_cache.go` and test it
with equivalent residual inputs in different order:

```go
func TestSolvePartitionResidualCachedHitsEquivalentSignature(t *testing.T) {
	topology := partitionSolverFixtureTopology()
	demands := []partitionDemand{
		{key: "a", quantity: 1, eligible: machine.NewCPUSet(0, 1), class: advisorBlockClassDedicated},
		{key: "b", quantity: 1, eligible: machine.NewCPUSet(2, 3), class: advisorBlockClassShared},
	}
	cache := newPartitionResidualSolveCache()
	searchBudget := defaultPartitionSearchBudget()

	first, hit, _, err := solvePartitionResidualCached(
		demands, topology, cache, defaultPartitionGraphBudget(), &searchBudget)
	require.NoError(t, err)
	require.False(t, hit)
	flowAfterMiss := searchBudget.flowOperations

	second, hit, _, err := solvePartitionResidualCached(
		[]partitionDemand{demands[1], demands[0]},
		topology, cache, defaultPartitionGraphBudget(), &searchBudget)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, flowAfterMiss, searchBudget.flowOperations)
	require.Equal(t, first, second)
}
```

Run:

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestSolvePartitionResidualCachedHitsEquivalentSignature$' \
  -count=1 -v
```

Expected: FAIL because the current residual loop invokes the solver for every
call and has no invocation-local cache.

- [ ] **Step 6: Wire cache lookup without bypassing proof validation**

Implement the cache helper:

```go
func solvePartitionResidualCached(
	demands []partitionDemand,
	topology *machine.CPUTopology,
	cache *partitionResidualSolveCache,
	graphBudget partitionGraphBudget,
	searchBudget *partitionSearchBudget,
) (map[string]machine.CPUSet, bool, partitionGraphBudget, error) {
	signature, err := newPartitionResidualSignature(demands, topology)
	if err != nil {
		return nil, false, graphBudget, err
	}
	if cached, found := cache.lookup(signature); found {
		return cached.assignments, true, graphBudget, cached.err
	}
	assignments, err := solveDisjointPartitionsWithBudgets(
		demands, topology, &graphBudget, searchBudget)
	if err == nil {
		cache.storeSuccess(signature, assignments)
	} else {
		cache.storeFailure(signature, err)
	}
	return assignments, false, graphBudget, err
}
```

At the start of `solveHardReclaimWithReplacement`, create one cache and one
search budget. Call `solvePartitionResidualCached` for each residual state,
handle budget errors exactly as in Task 1, continue only on
`errPartitionNoFeasibleAssignment`, and return every other error. Do not cache
`validationErr`: proof validity depends on the current replacement state even
when residual assignments are reusable.

- [ ] **Step 7: Verify cache semantics and deterministic results**

Run:

```bash
gofmt -w \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestPartitionResidual|TestSolvePartitionResidualCachedHitsEquivalentSignature|TestHardReclaimReplacementIsDeterministic)$' \
  -count=10
```

Expected: PASS in all ten repetitions; cache hits consume neither fresh graph
edges nor additional flow operations, and reversing demand order does not
change assignments.

- [ ] **Step 8: Commit the residual cache**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go
git commit -m "perf(qrm-cpu): cache equivalent replacement residual solves"
```

---

### Task 3: Record Search Diagnostics and Separate Infeasibility from Exhaustion

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_diagnostics.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_diagnostics_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go:59-82,613-833,953-1012,1026-1085`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go:968-1016,1139-1184`

- [ ] **Step 1: Add RED tests for complete and incomplete terminal outcomes**

Create `hard_reclaim_diagnostics_test.go`:

```go
package dynamicpolicy

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestHardReclaimDiagnosticsRecordCacheAndBudgetUsage(t *testing.T) {
	topology, demands, _, _ := productionNUMA2ReplacementFixture(t)

	result, err := solveHardReclaimWithReplacementDiagnosed(
		demands,
		topology.CPUDetails.CPUs(),
		topology,
		defaultHardReclaimReplacementOptions(),
	)

	require.NoError(t, err)
	require.NotNil(t, result.proof)
	require.Positive(t, result.diagnostics.GeneratedCandidateStates)
	require.Positive(t, result.diagnostics.DeduplicatedCandidateStates)
	require.Positive(t, result.diagnostics.TerminalStates)
	require.Positive(t, result.diagnostics.ResidualCacheMisses)
	require.Positive(t, result.diagnostics.ResidualCacheHits)
	require.Positive(t, result.diagnostics.MaxAssignmentEdgesInGraph)
	require.Positive(t, result.diagnostics.FlowOperations)
	require.Equal(t, result.proof.reclaimAfter.Intersection(
		result.proof.reclaimBefore).Size(), result.diagnostics.SelectedRetainedReclaimCPUs)
	require.NotEmpty(t, result.diagnostics.SelectedTouchedDonorGroups)
}

func TestHardReclaimCompleteInfeasibilityIsNotBudgetExhaustion(t *testing.T) {
	topology := partitionSolverFixtureTopology()
	demands := []partitionDemand{
		{
			key: "reclaim", quantity: 2, eligible: machine.NewCPUSet(0, 1),
			class: advisorBlockClassMandatoryReclaim,
		},
		{
			key: "dedicated", requestGroupKey: "pod/main", quantity: 2,
			requestQuantity: 2, eligible: machine.NewCPUSet(0, 1),
			class: advisorBlockClassDedicated,
		},
	}

	_, err := solveHardReclaimWithReplacementDiagnosed(
		demands, topology.CPUDetails.CPUs(), topology,
		defaultHardReclaimReplacementOptions())

	var noFeasible *hardReclaimNoFeasibleReplacement
	require.ErrorAs(t, err, &noFeasible)
	var exhausted *hardReclaimSearchBudgetExceeded
	require.False(t, errors.As(err, &exhausted))
	require.True(t, noFeasible.Diagnostics.Complete)
}

func TestHardReclaimBudgetExhaustionCarriesIncompleteDiagnostics(t *testing.T) {
	topology, demands, _, _ := productionNUMA2ReplacementFixture(t)
	options := defaultHardReclaimReplacementOptions()
	options.maxCandidateStates = 1

	_, err := solveHardReclaimWithReplacementDiagnosed(
		demands, topology.CPUDetails.CPUs(), topology, options)

	var exhausted *hardReclaimSearchBudgetExceeded
	require.ErrorAs(t, err, &exhausted)
	require.False(t, exhausted.Diagnostics.Complete)
	require.Equal(t, hardReclaimBudgetCandidateStates, exhausted.Budget)
}
```

- [ ] **Step 2: Run the diagnostics RED tests**

Run:

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestHardReclaim(Diagnostics|CompleteInfeasibility|BudgetExhaustion)' \
  -count=1 -v
```

Expected: FAIL to compile because the diagnosed result, diagnostics fields,
typed outcomes, and budget kind do not exist.

- [ ] **Step 3: Add immutable terminal diagnostics and typed errors**

Create `hard_reclaim_diagnostics.go`:

```go
package dynamicpolicy

import (
	"errors"
	"fmt"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type hardReclaimSearchBudgetKind string

const (
	hardReclaimBudgetCandidateStates hardReclaimSearchBudgetKind = "candidate_states"
	hardReclaimBudgetTerminalSolves  hardReclaimSearchBudgetKind = "terminal_solves"
	hardReclaimBudgetGraphEdges      hardReclaimSearchBudgetKind = "graph_edges"
	hardReclaimBudgetFlowOperations  hardReclaimSearchBudgetKind = "flow_operations"
)

type hardReclaimSearchDiagnostics struct {
	Complete                    bool
	GeneratedCandidateStates    int
	DeduplicatedCandidateStates int
	TerminalStates              int
	ResidualCacheHits           int
	ResidualCacheMisses         int
	MaxAssignmentEdgesInGraph   int
	FlowOperations              int
	SelectedRetainedReclaimCPUs int
	SelectedTouchedDonorGroups  []string
}

type hardReclaimNoFeasibleReplacement struct {
	Diagnostics hardReclaimSearchDiagnostics
	Cause       error
}

func (e *hardReclaimNoFeasibleReplacement) Error() string {
	return fmt.Sprintf("no feasible hard reclaim replacement: %v", e.Cause)
}

func (e *hardReclaimNoFeasibleReplacement) Unwrap() error { return e.Cause }

type hardReclaimSearchBudgetExceeded struct {
	Budget      hardReclaimSearchBudgetKind
	Diagnostics hardReclaimSearchDiagnostics
	Cause       error
}

func (e *hardReclaimSearchBudgetExceeded) Error() string {
	return fmt.Sprintf("hard reclaim %s budget exceeded: %v", e.Budget, e.Cause)
}

func (e *hardReclaimSearchBudgetExceeded) Unwrap() error { return e.Cause }

type hardReclaimDiagnosedResult struct {
	assignments map[string]machine.CPUSet
	proof       *hardReclaimReplacementProof
	diagnostics hardReclaimSearchDiagnostics
}
```

Keep fields unexported on `hardReclaimDiagnosedResult` because it is an
internal transport; keep diagnostic fields exported because typed errors
expose a stable inspectable snapshot.

- [ ] **Step 4: Count work at the ownership boundaries**

Refactor the existing function into a compatibility wrapper and one diagnosed
implementation:

```go
func solveHardReclaimWithReplacement(
	demands []partitionDemand,
	available machine.CPUSet,
	topology *machine.CPUTopology,
	options hardReclaimReplacementOptions,
) (map[string]machine.CPUSet, *hardReclaimReplacementProof, error) {
	result, err := solveHardReclaimWithReplacementDiagnosed(
		demands, available, topology, options)
	if err != nil {
		return nil, nil, err
	}
	return result.assignments, result.proof, nil
}
```

In `solveHardReclaimWithReplacementDiagnosed`:

- increment `GeneratedCandidateStates` before local/global deduplication;
- set `DeduplicatedCandidateStates` from the retained frontier sizes;
- increment `TerminalStates` for every candidate entering residual solve;
- increment cache hits/misses at the cache decision;
- after every miss, update
  `MaxAssignmentEdgesInGraph = max(old, graphBudget.assignmentEdges)`;
- copy `searchBudget.flowOperations` to `FlowOperations` before every return;
- derive selected retained CPUs and sorted touched donor group keys from the
  chosen proof;
- return diagnostics by value so callers cannot mutate in-flight accounting.

Use one finalizer to avoid missing counters on an error path:

```go
finish := func(complete bool) hardReclaimSearchDiagnostics {
	diagnostics.Complete = complete
	diagnostics.FlowOperations = searchBudget.flowOperations
	diagnostics.SelectedTouchedDonorGroups =
		append([]string(nil), diagnostics.SelectedTouchedDonorGroups...)
	return diagnostics
}
```

Keep `solvePartitionResidualCached` returning its `hit` boolean. The
replacement loop converts that boolean into
`result.diagnostics.ResidualCacheHits` and `ResidualCacheMisses`; the direct
Task 2 cache test continues to assert that a hit consumes no additional flow
operations.

- [ ] **Step 5: Map each exhausted limit and complete miss to the correct type**

Replace generic `hardReclaimBudgetError` calls with:

```go
func hardReclaimBudgetExceeded(
	kind hardReclaimSearchBudgetKind,
	diagnostics hardReclaimSearchDiagnostics,
	cause error,
) error {
	return &hardReclaimSearchBudgetExceeded{
		Budget: kind, Diagnostics: diagnostics, Cause: cause,
	}
}
```

Map candidate-state, terminal-solve, graph-edge, and flow-operation limits to
their matching `hardReclaimSearchBudgetKind`. Preserve the original partition
budget cause so these remain true:

```go
errors.Is(err, errPartitionAssignmentEdgeBudget)
errors.Is(err, errPartitionFlowOperationBudget)
```

Only after every deduplicated candidate has been evaluated without truncation,
return:

```go
return hardReclaimDiagnosedResult{}, &hardReclaimNoFeasibleReplacement{
	Diagnostics: finish(true),
	Cause: &hardReclaimSelectionError{
		reason: hardReclaimFailureDonorFloor,
		cause:  fmt.Errorf("no global hard reclaim replacement is feasible"),
	},
}
```

Do not add heuristic pruning. Existing candidate ordering and
`hardReclaimReplacementResultLess` remain the complete ordering.

- [ ] **Step 6: Emit one bounded diagnostic summary at the production call site**

Add an error extractor in `hard_reclaim_diagnostics.go`:

```go
func hardReclaimDiagnosticsFromError(err error) (
	hardReclaimSearchDiagnostics,
	bool,
) {
	var noFeasible *hardReclaimNoFeasibleReplacement
	if errors.As(err, &noFeasible) {
		return noFeasible.Diagnostics, true
	}
	var exhausted *hardReclaimSearchBudgetExceeded
	if errors.As(err, &exhausted) {
		return exhausted.Diagnostics, true
	}
	return hardReclaimSearchDiagnostics{}, false
}
```

In `pinHardReclaimPartitionDemands`, call the diagnosed implementation and
emit one low-cardinality structured summary on success or typed terminal
failure:

```go
replacement, replacementErr := solveHardReclaimWithReplacementDiagnosed(
	demands, available, topology, newHardReclaimReplacementOptions())
diagnostics := replacement.diagnostics
if fromError, ok := hardReclaimDiagnosticsFromError(replacementErr); ok {
	diagnostics = fromError
}
general.InfoS(
	"finished hard reclaim replacement search",
	"complete", diagnostics.Complete,
	"generatedCandidateStates", diagnostics.GeneratedCandidateStates,
	"deduplicatedCandidateStates", diagnostics.DeduplicatedCandidateStates,
	"terminalStates", diagnostics.TerminalStates,
	"residualCacheHits", diagnostics.ResidualCacheHits,
	"residualCacheMisses", diagnostics.ResidualCacheMisses,
	"maxAssignmentEdgesInGraph", diagnostics.MaxAssignmentEdgesInGraph,
	"flowOperations", diagnostics.FlowOperations,
	"selectedRetainedReclaimCPUs", diagnostics.SelectedRetainedReclaimCPUs,
	"selectedTouchedDonorGroupCount", len(diagnostics.SelectedTouchedDonorGroups),
)
if replacementErr != nil {
	return nil, fmt.Errorf(
		"hard reclaim fast path failed: %v; replacement failed: %w",
		err, replacementErr)
}
return pinPartitionDemandsToAssignments(demands, replacement.assignments)
```

Do not log signatures, Pod UIDs, group names, or CPU-set contents. The summary
is one event per replacement invocation and has fixed field names.

- [ ] **Step 7: Verify diagnostics, typed outcomes, and old error compatibility**

Run:

```bash
gofmt -w \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_diagnostics.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_diagnostics_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestHardReclaim' -count=1
```

Expected: PASS. Complete infeasibility has `Diagnostics.Complete == true`;
every exhausted limit has `Complete == false`; graph/flow budget errors still
match their existing sentinels through `errors.Is`.

- [ ] **Step 8: Commit diagnostics and typed completion**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_diagnostics.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_diagnostics_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go
git commit -m "feat(qrm-cpu): report replacement search diagnostics"
```

---

### Task 4: Replay Production Fixtures and Prove the Pre-Commit Boundary

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_fixture_test.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/testdata/hard_reclaim_replacement/affected-numa2-boundary.json`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/testdata/hard_reclaim_replacement/multi-numa-complete-core-deficit.json`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go:585-675`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go:1987-2144`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go:569-667`

- [ ] **Step 1: Add the fixture schema and loader as RED test code**

Create `hard_reclaim_fixture_test.go`:

```go
package dynamicpolicy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type hardReclaimFixtureCPU struct {
	ID         int `json:"id"`
	NUMANodeID int `json:"numaNodeID"`
	SocketID   int `json:"socketID"`
	CoreID     int `json:"coreID"`
}

type hardReclaimFixtureDemand struct {
	Key             string  `json:"key"`
	RequestGroupKey string  `json:"requestGroupKey"`
	Quantity        int     `json:"quantity"`
	RequestQuantity float64 `json:"requestQuantity"`
	Eligible        string  `json:"eligible"`
	Preferred       string  `json:"preferred"`
	Class           string  `json:"class"`
}

type hardReclaimGeneratedTopology struct {
	NumCPUs     int `json:"numCPUs"`
	NumSockets  int `json:"numSockets"`
	NumNUMAs    int `json:"numNUMAs"`
}

type hardReclaimFixture struct {
	Name                     string                     `json:"name"`
	GeneratedTopology        *hardReclaimGeneratedTopology `json:"generatedTopology,omitempty"`
	CPUs                     []hardReclaimFixtureCPU    `json:"cpus"`
	Demands                  []hardReclaimFixtureDemand `json:"demands"`
	Available                string                     `json:"available"`
	ExpectedReclaim          string                     `json:"expectedReclaim"`
	ExpectedAssignmentSizes  map[string]int             `json:"expectedAssignmentSizes"`
	ExpectCompleteNoFeasible bool                       `json:"expectCompleteNoFeasible"`
}

func loadHardReclaimFixture(t *testing.T, name string) (
	hardReclaimFixture,
	*machine.CPUTopology,
	[]partitionDemand,
	machine.CPUSet,
) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(
		"testdata", "hard_reclaim_replacement", name+".json"))
	require.NoError(t, err)
	var fixture hardReclaimFixture
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.NotEqual(t, fixture.GeneratedTopology != nil, len(fixture.CPUs) > 0,
		"fixture must set exactly one topology representation")

	var topology *machine.CPUTopology
	if fixture.GeneratedTopology != nil {
		generated := fixture.GeneratedTopology
		topology, err = machine.GenerateDummyCPUTopology(
			generated.NumCPUs, generated.NumSockets, generated.NumNUMAs)
		require.NoError(t, err)
	} else {
		details := make(machine.CPUDetails, len(fixture.CPUs))
		numaIDs, sockets := map[int]struct{}{}, map[int]struct{}{}
		cores := map[[2]int]struct{}{}
		for _, cpu := range fixture.CPUs {
			details[cpu.ID] = machine.CPUTopoInfo{
				NUMANodeID: cpu.NUMANodeID,
				SocketID: cpu.SocketID,
				CoreID: cpu.CoreID,
			}
			numaIDs[cpu.NUMANodeID] = struct{}{}
			sockets[cpu.SocketID] = struct{}{}
			cores[[2]int{cpu.SocketID, cpu.CoreID}] = struct{}{}
		}
		topology = &machine.CPUTopology{
			NumCPUs: len(details), NumCores: len(cores),
			NumSockets: len(sockets), NumNUMANodes: len(numaIDs),
			CPUDetails: details,
		}
	}

	demands := make([]partitionDemand, 0, len(fixture.Demands))
	totalQuantity := 0
	for _, item := range fixture.Demands {
		eligible, err := machine.Parse(item.Eligible)
		require.NoError(t, err)
		preferred, err := machine.Parse(item.Preferred)
		require.NoError(t, err)
		require.True(t, preferred.IsSubsetOf(eligible))
		require.True(t, eligible.IsSubsetOf(topology.CPUDetails.CPUs()))
		demands = append(demands, partitionDemand{
			key: item.Key, requestGroupKey: item.RequestGroupKey,
			quantity: item.Quantity, requestQuantity: item.RequestQuantity,
			eligible: eligible, preferred: preferred,
			class: advisorBlockClass(item.Class),
		})
		totalQuantity += item.Quantity
	}
	available, err := machine.Parse(fixture.Available)
	require.NoError(t, err)
	require.True(t, available.IsSubsetOf(topology.CPUDetails.CPUs()))
	require.LessOrEqual(t, totalQuantity, available.Size())
	return fixture, topology, demands, available
}
```

Add the table-driven RED test:

```go
func TestHardReclaimProductionFixturesCompleteWithinBudgets(t *testing.T) {
	for _, name := range []string{
		"affected-numa2-boundary",
		"multi-numa-complete-core-deficit",
	} {
		name := name
		t.Run(name, func(t *testing.T) {
			fixture, topology, demands, available :=
				loadHardReclaimFixture(t, name)
			var first map[string]machine.CPUSet

			for run := 0; run < 20; run++ {
				result, err := solveHardReclaimWithReplacementDiagnosed(
					demands, available, topology,
					defaultHardReclaimReplacementOptions())
				if fixture.ExpectCompleteNoFeasible {
					var noFeasible *hardReclaimNoFeasibleReplacement
					require.ErrorAs(t, err, &noFeasible)
					require.True(t, noFeasible.Diagnostics.Complete)
					continue
				}

				require.NoError(t, err)
				require.True(t, result.diagnostics.Complete)
				require.LessOrEqual(t,
					result.diagnostics.MaxAssignmentEdgesInGraph,
					partitionAssignmentEdgeBudget)
				require.LessOrEqual(t,
					result.diagnostics.FlowOperations,
					partitionFlowOperationBudget)
				for key, size := range fixture.ExpectedAssignmentSizes {
					require.Equal(t, size, result.assignments[key].Size())
				}
				expectedReclaim, parseErr := machine.Parse(fixture.ExpectedReclaim)
				require.NoError(t, parseErr)
				require.Equal(t, expectedReclaim, result.proof.reclaimAfter)
				requireCoreAligned(t, topology, result.proof.reclaimAfter)
				if run == 0 {
					first = clonePartitionAssignments(result.assignments)
				} else {
					require.Equal(t, first, result.assignments)
				}
			}
		})
	}
}
```

- [ ] **Step 2: Store the sanitized captured NUMA-2 fixture**

Create `testdata/hard_reclaim_replacement/affected-numa2-boundary.json` from
the exact values currently in `productionNUMA2ReplacementFixture`. Its full
content uses the deterministic topology generator parameters and the captured
CPU sets:

```json
{
  "name": "affected-numa2-boundary",
  "generatedTopology": {
    "numCPUs": 128,
    "numSockets": 2,
    "numNUMAs": 4
  },
  "available": "32-47,96-111",
  "expectedReclaim": "32,44,46-47,96,108,110-111",
  "expectedAssignmentSizes": {
    "reclaim-numa-2": 8,
    "dedicated-numa-2": 24
  },
  "expectCompleteNoFeasible": false,
  "demands": [
    {
      "key": "reclaim-numa-2",
      "requestGroupKey": "",
      "quantity": 8,
      "requestQuantity": 0,
      "eligible": "32-47,96-111",
      "preferred": "32,46-47,96,108-111",
      "class": "mandatory-reclaim"
    },
    {
      "key": "dedicated-numa-2",
      "requestGroupKey": "pod/main",
      "quantity": 24,
      "requestQuantity": 62,
      "eligible": "32-47,96-111",
      "preferred": "33-45,97-107",
      "class": "dedicated"
    }
  ]
}
```

The loader must reject a fixture that sets both `generatedTopology` and
`cpus`, or neither. Verify the serialized class strings against
`advisorBlockClassMandatoryReclaim` and `advisorBlockClassDedicated`.

- [ ] **Step 3: Store the multi-NUMA complete-core deficit fixture**

Create
`testdata/hard_reclaim_replacement/multi-numa-complete-core-deficit.json`.
Use `machine.GenerateDummyCPUTopology(64, 2, 2)` through the fixture's
`generatedTopology` field, and store:

```json
{
  "name": "multi-numa-complete-core-deficit",
  "generatedTopology": {
    "numCPUs": 64,
    "numSockets": 2,
    "numNUMAs": 2
  },
  "available": "0-63",
  "expectedReclaim": "0-2,16,32-34,48",
  "expectedAssignmentSizes": {
    "reclaim-numa-0": 4,
    "reclaim-numa-1": 4,
    "dedicated-a": 28,
    "dedicated-b": 28
  },
  "expectCompleteNoFeasible": false,
  "demands": [
    {
      "key": "reclaim-numa-0",
      "requestGroupKey": "",
      "quantity": 4,
      "requestQuantity": 0,
      "eligible": "0-15,32-47",
      "preferred": "0-2,32",
      "class": "mandatory-reclaim"
    },
    {
      "key": "reclaim-numa-1",
      "requestGroupKey": "",
      "quantity": 4,
      "requestQuantity": 0,
      "eligible": "16-31,48-63",
      "preferred": "16-18,48",
      "class": "mandatory-reclaim"
    },
    {
      "key": "dedicated-a",
      "requestGroupKey": "pod-a/main",
      "quantity": 28,
      "requestQuantity": 28,
      "eligible": "0-63",
      "preferred": "3-15,17,35-47,49",
      "class": "dedicated"
    },
    {
      "key": "dedicated-b",
      "requestGroupKey": "pod-b/main",
      "quantity": 28,
      "requestQuantity": 28,
      "eligible": "0-63",
      "preferred": "18-31,50-63",
      "class": "dedicated"
    }
  ]
}
```

Run the loader immediately after creating each file. It must validate every
preferred set as a subset of its eligible set, every referenced CPU as present
in the topology, and the assignment-quantity sum as no greater than the
available CPU count.

- [ ] **Step 4: Run the fixture RED test**

Run:

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestHardReclaimProductionFixturesCompleteWithinBudgets$' \
  -count=1 -v
```

Expected before Tasks 1-3: FAIL on the affected fixture with
`partition graph edge budget exceeded`, or fail because diagnostics cannot
prove completion. Expected after Tasks 1-3: PASS for both fixtures and all 20
determinism repetitions.

- [ ] **Step 5: Replace the duplicated inline production helper**

Change `productionNUMA2ReplacementFixture` in
`hard_reclaim_partition_test.go` to delegate to the loader while preserving its
existing return signature:

```go
func productionNUMA2ReplacementFixture(t *testing.T) (
	*machine.CPUTopology,
	[]partitionDemand,
	machine.CPUSet,
	machine.CPUSet,
) {
	t.Helper()
	_, topology, demands, _ :=
		loadHardReclaimFixture(t, "affected-numa2-boundary")
	return topology, demands, demands[0].preferred.Clone(), demands[1].preferred.Clone()
}
```

This keeps the source-pool and advisor transaction tests on the exact same
fixture as the direct solver tests and removes hand-maintained duplicate CPU
lists.

- [ ] **Step 6: Add a RED transaction test for zero mutation on solver failure**

In `policy_advisor_handler_test.go`, add a sibling of
`TestAdvisorReplacementTransactionRetainsExactTargetAcrossRetry` that:

1. loads `affected-numa2-boundary`;
2. creates the policy/response through
   `productionReplacementSourcePoolFixture`;
3. snapshots `state.GetRevision()`, `state.GetPodEntries()`,
   `state.GetMachineState()`, the staging/active checkpoint bytes if present,
   and `currentAdvisorPostCommitTarget()`;
4. forces `maxPartitionAssignmentEdges = 1` through the package-private
   replacement-options seam used by the source-pool planner;
5. calls the advisor planning path;
6. asserts `errors.Is(err, errPartitionAssignmentEdgeBudget)`;
7. asserts every snapshot is byte-for-byte/deep-equal unchanged and no new
   checkpoint file exists.

Use one injectable package variable, restored with `t.Cleanup`, rather than a
production flag:

```go
var newHardReclaimReplacementOptions = defaultHardReclaimReplacementOptions
```

The RED assertion core is:

```go
beforeRevision := policy.state.GetRevision()
beforeEntries := policy.state.GetPodEntries()
beforeMachine := policy.state.GetMachineState()
beforeTarget := policy.currentAdvisorPostCommitTarget()

_, err := policy.planDisjointAdvisorBlocks(resp, true)

require.ErrorIs(t, err, errPartitionAssignmentEdgeBudget)
require.Equal(t, beforeRevision, policy.state.GetRevision())
require.Equal(t, beforeEntries, policy.state.GetPodEntries())
require.Equal(t, beforeMachine, policy.state.GetMachineState())
require.Same(t, beforeTarget, policy.currentAdvisorPostCommitTarget())
require.NoFileExists(t, filepath.Join(
	policy.advisorPostCommitCheckpointDir, advisorPostCommitCheckpointName))
```

Run:

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestAdvisorReplacementSolverFailureDoesNotStageOrCommit$' \
  -count=1 -v
```

Expected: FAIL to compile until the options seam is used by
`pinHardReclaimPartitionDemands`; after wiring it, PASS with no revision, WAL,
or target mutation.

- [ ] **Step 7: Run all solver, source-pool, and transaction fixture tests**

Run:

```bash
gofmt -w \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_fixture_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestHardReclaimProductionFixturesCompleteWithinBudgets|TestPlanDisjointAdvisorBlocksRepairsDedicatedReclaimBoundary|TestPlanDisjointAdvisorBlocksPreservesReplacementAssignment|TestAdvisorReplacementSolverFailureDoesNotStageOrCommit)$' \
  -count=10
```

Expected: PASS in all ten repetitions. Both fixtures finish below every
configured search limit, produce deterministic assignments, preserve NUMA
quantity and SMT closure, and the forced budget failure remains entirely
before the revision/WAL/post-commit boundary.

- [ ] **Step 8: Commit production fixtures and integration proof**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_fixture_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/testdata/hard_reclaim_replacement/affected-numa2-boundary.json \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/testdata/hard_reclaim_replacement/multi-numa-complete-core-deficit.json
git commit -m "test(qrm-cpu): replay replacement production fixtures"
```

---

## Final Verification

- [ ] **Run formatting and whitespace checks**

```bash
gofmt -w \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_partition_solver_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_residual_cache_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_diagnostics.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_diagnostics_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_fixture_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go
git diff --check
```

Expected: `gofmt` produces no subsequent diff and `git diff --check` exits 0
without output.

- [ ] **Run focused tests repeatedly**

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestSolveDisjointPartitions|TestPartitionResidual|TestHardReclaim|TestPlanDisjointAdvisorBlocksRepairsDedicatedReclaimBoundary|TestPlanDisjointAdvisorBlocksPreservesReplacementAssignment|TestAdvisorReplacementSolverFailureDoesNotStageOrCommit)$' \
  -count=20
```

Expected: PASS for all 20 repetitions.

- [ ] **Run the full dynamicpolicy package with repository-required Mockey flags**

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... \
  -count=1
```

Expected: PASS.

- [ ] **Run focused race tests**

```bash
go test -race -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestHardReclaimProductionFixturesCompleteWithinBudgets|TestSolvePartitionResidualCachedHitsEquivalentSignature|TestAdvisorReplacementSolverFailureDoesNotStageOrCommit)$' \
  -count=1
```

Expected: PASS with no race report. Although cache ownership is synchronous,
this gate protects future call-site changes and the package-level options seam
used by the transaction test.

- [ ] **Run vet and inspect the four atomic commits**

```bash
go vet ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
git log -4 --oneline
git status --short
```

Expected: `go vet` exits 0; the log shows, in order:

1. `fix(qrm-cpu): separate graph and search solver budgets`
2. `perf(qrm-cpu): cache equivalent replacement residual solves`
3. `feat(qrm-cpu): report replacement search diagnostics`
4. `test(qrm-cpu): replay replacement production fixtures`

The worktree is clean.
