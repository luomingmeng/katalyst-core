# Container Lifecycle Proof and Safe Replan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one strict fresh Pod snapshot the sole lifecycle-classification owner for a cpuset-topology attempt, validate that frozen decision after physical convergence, and safely recompile stale attempts without returning an avoidable Allocate failure.

**Architecture:** The cpuset-topology expected-view compiler emits ordered resolved, pending, and retired container proofs. Finalization validates those proofs against exactly one post-execution strict snapshot and reads physical CPU sets only from the final DAG snapshot; it never reclassifies raw desired entries. The topology coordinator returns an explicit `ReplanDisposition`, and the plugin owns a bounded recompile loop that reuses one deadline and cumulative physical-work budget.

**Tech Stack:** Go 1.18, Katalyst QRM dynamic CPU policy, Bulkhead topology coordinator, cgroup v1/v2, Testify, Mockey-compatible tests, Go race detector.

---

## File Map

- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/container_lifecycle_proof.go`
  - Own immutable container proof types, deterministic ordering/digest, one-shot post-execution validation, and AppliedView materialization.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/container_lifecycle_proof_test.go`
  - Unit tests for digest, validation matrix, deep-copy behavior, and one strict snapshot.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
  - Make `buildExpectedCPUSetByRel` emit all proof states, delete finalizer reclassification, split `adjustOnce`, and own the bounded recompile loop.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/pending_fresh_scope_test.go`
  - RED tests for resolved, pending, and retired proof generation.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`
  - RED tests for finalization, stale publication, recompile, and full admission/post-advisor behavior.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/replan.go`
  - Own `ReplanDisposition` and cumulative `AdjustmentBudget`.
- Create `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/replan_test.go`
  - Contract tests for disposition validation and cumulative budgets.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
  - Return explicit replan authorization and preserve a verified final snapshot on publication drift.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator_test.go`
  - RED tests for pre-write stale, publication stale, hard publication errors, and no internal stale-proof retry.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go`
  - Mark verified rollback as replan-safe and all incomplete rollback states as terminal.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go`
  - RED tests for rollback disposition and stale error-chain preservation.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
  - Delete the topology-specific generic per-handler retry owner.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`
  - Prove DynamicPolicy does not interpret topology-private stale markers.

---

### Task 1: Freeze Complete Container Lifecycle Proofs

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/container_lifecycle_proof.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/container_lifecycle_proof_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/pending_fresh_scope_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`

- [ ] **Step 1: Add RED tests for all three lifecycle states**

```go
func TestBuildExpectedCPUSetByRelCarriesResolvedPendingAndRetiredProofs(t *testing.T)
func TestBuildExpectedCPUSetByRelOrdersLifecycleProofsDeterministically(t *testing.T)
func TestLifecycleProofDigestIgnoresInputMapOrder(t *testing.T)
func TestLifecycleProofsCloneDesiredCPUSet(t *testing.T)
```

Build one desired view containing:

```text
primary/resolved: fresh Spec owns name, fresh status ID and leaf exist
primary/pending:  fresh Spec owns name, status ID exists, leaf is typed ENOENT
primary/retired:  fresh Spec no longer owns name and old leaf is typed ENOENT
```

Assert every non-empty desired entry appears exactly once in `OrderedProofs`.

- [ ] **Step 2: Run RED**

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -run 'Test(BuildExpectedCPUSetByRelCarries|BuildExpectedCPUSetByRelOrders|LifecycleProof)' \
  -count=1 -v
```

Expected: compile failure because `containerLifecycleProofSet` and
`LifecycleProofs` do not exist.

- [ ] **Step 3: Add immutable proof types**

```go
type containerLifecycleState uint8

const (
	containerLifecycleResolved containerLifecycleState = iota + 1
	containerLifecyclePending
	containerLifecycleRetired
)

type containerLifecycleProof struct {
	PodUID        string
	ContainerName string
	ContainerID   string
	RelativePath  string
	DesiredCPUSet machine.CPUSet
	State         containerLifecycleState
}

type containerLifecycleProofSet struct {
	SourceSnapshotDigest string
	OrderedProofs        []containerLifecycleProof
}

func (p containerLifecycleProof) clone() containerLifecycleProof {
	p.DesiredCPUSet = p.DesiredCPUSet.Clone()
	return p
}
```

Extend the existing result:

```go
type expectedCPUSetBuildResult struct {
	ExpectedByRel     map[string]machine.CPUSet
	DeferredLeafByRel map[string]machine.CPUSet
	PendingByPod      []pendingContainerCPUSet
	LifecycleProofs   containerLifecycleProofSet
}
```

- [ ] **Step 4: Replace implicit retirement with explicit classification**

Introduce:

```go
type podLifecycleClassification struct {
	Resolved []resolvedContainerCPUSet
	Pending  []pendingContainerCPUSet
	Retired  []containerLifecycleProof
	ScopeRel string
	QOSClass v1.PodQOSClass
	Stale    bool
}
```

Change `filterPodOutcomesAgainstFreshPod` to return this value. Every branch
that currently drops a fresh-unowned entry with `continue` appends a retired
proof instead.

- [ ] **Step 5: Canonically order and digest only involved Pods**

```go
func freezeContainerLifecycleProofs(
	proofs []containerLifecycleProof,
	freshPods map[string]*v1.Pod,
) (containerLifecycleProofSet, error)
```

Sort by Pod UID, container name, container ID, relative path, state, and CPUSet
string. Encode length-prefixed fields into SHA-256. Include only Pod UIDs
present in `proofs`, normalized Spec owner names, and status container IDs.

- [ ] **Step 6: Run focused tests**

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -run 'Test(BuildExpectedCPUSetByRelCarries|BuildExpectedCPUSetByRelOrders|LifecycleProof)' \
  -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/container_lifecycle_proof.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/container_lifecycle_proof_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/pending_fresh_scope_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go
git commit -m "fix(qrm-cpu): freeze container lifecycle proofs"
```

---

### Task 2: Validate Proofs from One Post-Execution Snapshot

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/container_lifecycle_proof.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/container_lifecycle_proof_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`

- [ ] **Step 1: Add RED validation matrix**

```go
func TestValidateContainerLifecycleProofsUsesOneStrictSnapshot(t *testing.T)
func TestValidateContainerLifecycleProofsAcceptsStableResolvedProof(t *testing.T)
func TestValidateContainerLifecycleProofsOmitsConfirmedRetiredProof(t *testing.T)
func TestValidateContainerLifecycleProofsKeepsStablePendingProofDeferred(t *testing.T)
func TestValidateContainerLifecycleProofsRejectsContainerIDChange(t *testing.T)
func TestValidateContainerLifecycleProofsRejectsRetiredOwnerReappearance(t *testing.T)
func TestValidateContainerLifecycleProofsKeepsMaterializedPendingProofDeferred(t *testing.T)
func TestValidateContainerLifecycleProofsPreservesSnapshotTransportError(t *testing.T)
```

Assert strict `GetPodList` count is exactly one and finalization performs zero
`ResolveContainerRelPathWithContext` calls.

- [ ] **Step 2: Run RED**

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -run '^TestValidateContainerLifecycleProofs' -count=1 -v
```

Expected: compile failure for missing validator and stale-proof error.

- [ ] **Step 3: Add validation result and typed stale error**

```go
type validatedContainerLifecycleProofSet struct {
	SourceSnapshotDigest string
	ValidationDigest     string
	OrderedProofs        []containerLifecycleProof
}

type containerLifecycleProofStaleError struct {
	PodUID        string
	ContainerName string
	FrozenID      string
	CurrentID     string
	Reason        string
}

func (e *containerLifecycleProofStaleError) Error() string
func (e *containerLifecycleProofStaleError) Unwrap() error {
	return topology.ErrCoordinatorPlanStale
}
```

- [ ] **Step 4: Implement one-shot validation**

```go
func validateContainerLifecycleProofs(
	ctx context.Context,
	metaServer *metaserver.MetaServer,
	frozen containerLifecycleProofSet,
) (validatedContainerLifecycleProofSet, error)
```

The function obtains one strict fresh `GetPodList`, indexes it by UID, and
validates without resolving cgroup paths:

```text
resolved: owner exists and current ID equals frozen ID
pending: owner exists and current ID/absence is unchanged
retired: owner remains absent
```

An identity change returns `containerLifecycleProofStaleError`. Finalization
does not probe whether a stable pending leaf materialized; it remains deferred
until the next periodic compile. Snapshot transport or decode failure is
returned unchanged.

- [ ] **Step 5: Materialize AppliedView only from validated proofs**

```go
func containerCPUSetByPodFromFinalSnapshot(
	snapshot *topology.CompleteSnapshot,
	proofs validatedContainerLifecycleProofSet,
	deferredCleanupRels map[string]struct{},
) (map[string]map[string]machine.CPUSet, error)
```

Rules:

```text
retired: omit
pending: omit and preserve deferred protection
resolved: read RelativePath from final snapshot and publish that CPUSet
missing resolved final leaf: stale error
EACCES/EIO/malformed final snapshot: hard error
```

- [ ] **Step 6: Delete the old finalization owner**

Delete lifecycle classification and direct MetaServer/path resolution from
`containerCPUSetByPodFromFinalSnapshotWithDeferredCleanup`. Convert
`appliedViewFromFinalSnapshotWithContext` and the ParentSafe variant to accept
`containerLifecycleProofSet`.

Do not keep the raw DesiredView loop as a fallback.

- [ ] **Step 7: Run focused tests**

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -run 'Test(ValidateContainerLifecycleProofs|Finalization)' -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/container_lifecycle_proof.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/container_lifecycle_proof_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go
git commit -m "fix(qrm-cpu): validate lifecycle proofs at finalization"
```

---

### Task 3: Add Explicit Replan Disposition

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/replan.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/replan_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator_test.go`

- [ ] **Step 1: Add RED contract tests**

```go
func TestReplanDispositionDefaultsToNotAllowed(t *testing.T)
func TestTopologyCoordinatorPreWriteStaleAuthorizesNoWriteReplan(t *testing.T)
func TestTopologyCoordinatorPublicationStalePreservesVerifiedFinalState(t *testing.T)
func TestTopologyCoordinatorHardPublicationErrorDisallowsReplan(t *testing.T)
func TestTopologyCoordinatorDoesNotRetryStalePublicationWithOldProof(t *testing.T)
```

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Test(ReplanDisposition|TopologyCoordinator.*Replan|TopologyCoordinatorPublication)' \
  -count=1 -v
```

Expected: compile failure for `ReplanDisposition` and `Published`.

- [ ] **Step 3: Add explicit result types**

```go
type ReplanDisposition uint8

const (
	ReplanNotAllowed ReplanDisposition = iota
	ReplanSafeNoPhysicalWrites
	ReplanSafeAfterVerifiedRollback
	ReplanSafeFromVerifiedFinalState
)

func (d ReplanDisposition) AllowsReplan() bool {
	return d == ReplanSafeNoPhysicalWrites ||
		d == ReplanSafeAfterVerifiedRollback ||
		d == ReplanSafeFromVerifiedFinalState
}
```

Extend `ConvergenceResult`:

```go
ReplanDisposition ReplanDisposition
Published         bool
```

The zero value is intentionally terminal.

- [ ] **Step 4: Return publication stale to the plugin**

When publication returns stale after a complete current final snapshot:

```go
res.Published = false
res.ReplanDisposition = ReplanSafeFromVerifiedFinalState
return *res, err
```

Preserve `FinalSnapshot` and `FinalSnapshotCurrent`. Do not internally retry
with the old expected map or proof. A hard publication error sets
`ReplanNotAllowed`.

- [ ] **Step 5: Run focused tests and commit**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Test(ReplanDisposition|TopologyCoordinator.*Replan|TopologyCoordinatorPublication)' \
  -count=1
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/replan.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/replan_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator_test.go
git commit -m "fix(qrm-cpu): expose safe topology replan disposition"
```

---

### Task 4: Authorize Replan Only after Verified Physical Recovery

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go`

- [ ] **Step 1: Add RED rollback tests**

```go
func TestVerifiedRollbackAuthorizesReplan(t *testing.T)
func TestRollbackFailurePreservesStaleCauseButDisallowsReplan(t *testing.T)
func TestRollbackVerificationFailureDisallowsReplan(t *testing.T)
func TestPartialRollbackDisallowsReplan(t *testing.T)
func TestPreWriteStaleAuthorizesReplanWithoutRollback(t *testing.T)
```

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Test(VerifiedRollback|RollbackFailurePreserves|RollbackVerification|PartialRollback|PreWriteStale)' \
  -count=1 -v
```

Expected: disposition remains `ReplanNotAllowed`.

- [ ] **Step 3: Set disposition from physical evidence**

Update the execution path:

```go
if noPhysicalWrites {
	res.ReplanDisposition = ReplanSafeNoPhysicalWrites
	return staleErr
}

rollbackErr := w.rollbackTracePrefix(...)
if rollbackErr == nil && rollbackVerificationComplete {
	res.ReplanDisposition = ReplanSafeAfterVerifiedRollback
	return staleErr
}

res.ReplanDisposition = ReplanNotAllowed
return newExecutionRollbackError(staleErr, rollbackErr)
```

Do not infer safety from `errors.Is`. Preserve stale error chains for
diagnostics even when disposition is terminal.

- [ ] **Step 4: Run focused tests and commit**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Test(VerifiedRollback|RollbackFailurePreserves|RollbackVerification|PartialRollback|PreWriteStale)' \
  -count=1
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go
git commit -m "fix(qrm-cpu): authorize replan from verified physical state"
```

---

### Task 5: Move Bounded Recompile into the Topology Plugin

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/replan.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/replan_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`

- [ ] **Step 1: Add RED plugin-owned recompile tests**

```go
func TestCPUSetTopologyPluginRecompilesPreWriteStalePlan(t *testing.T)
func TestCPUSetTopologyPluginRecompilesAfterVerifiedRollback(t *testing.T)
func TestCPUSetTopologyPluginRecompilesFromVerifiedFinalState(t *testing.T)
func TestCPUSetTopologyPluginDoesNotRecompileTerminalStale(t *testing.T)
func TestCPUSetTopologyPluginReplanStartsFromPristineDesiredView(t *testing.T)
func TestCPUSetTopologyPluginReplansShareDeadlineAndWriteBudget(t *testing.T)
func TestCPUSetTopologyPluginContinuousChurnExhaustsReplanBudget(t *testing.T)
```

- [ ] **Step 2: Run RED**

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -run '^TestCPUSetTopologyPlugin(Recompiles|DoesNotRecompile|Replan|Continuous)' \
  -count=1 -v
```

Expected: the first stale attempt returns immediately.

- [ ] **Step 3: Add cumulative adjustment budget**

```go
type AdjustmentBudget struct {
	deadline       time.Time
	maxReplans     int
	replans        int
	maxWrites      int
	physicalWrites int
	rollbackWrites int
}

func (b *AdjustmentBudget) ConsumeReplan() bool
func (b *AdjustmentBudget) RecordPhysicalWrites(int) error
func (b *AdjustmentBudget) RecordRollbackWrites(int) error
func (b *AdjustmentBudget) RemainingConvergenceBudget() ConvergenceBudget
```

Use the original context deadline. Never extend it on replan.

- [ ] **Step 4: Split one attempt from the owner loop**

```go
func (p *CPUSetTopologyPlugin) adjustOnce(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
	budget *topology.AdjustmentBudget,
) (topology.ConvergenceResult, error)
```

Move the current `CPUSetAdjustmentHandler` body into `adjustOnce`.

- [ ] **Step 5: Implement the owner loop**

```go
func (p *CPUSetTopologyPlugin) CPUSetAdjustmentHandler(
	ctx context.Context,
	in bulkheadapi.HandlerContext,
) error {
	budget := topology.NewAdjustmentBudget(ctx, p.cfg.TopologyConvergenceBudget)
	pristineDesired := in.DesiredView.DeepCopy()

	for {
		attempt := in
		attempt.DesiredView = pristineDesired.DeepCopy()

		result, err := p.adjustOnce(ctx, attempt, budget)
		if err == nil {
			return nil
		}
		if !result.ReplanDisposition.AllowsReplan() || !budget.ConsumeReplan() {
			return err
		}
	}
}
```

Rebuild expected view, lifecycle proofs, siblings, DAG, protections, and final
publication callbacks on every attempt. Do not reuse mutable derived state.

- [ ] **Step 6: Run focused tests and commit**

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -run '^TestCPUSetTopologyPlugin(Recompiles|DoesNotRecompile|Replan|Continuous)' \
  -count=1
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/replan.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/replan_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go
git commit -m "fix(qrm-cpu): safely recompile stale topology plans"
```

---

### Task 6: Delete the Generic Topology Retry Owner

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`

- [ ] **Step 1: Add RED ownership test**

```go
func TestRunCPUSetAdjustmentHandlersDoesNotInterpretTopologyReplanErrors(t *testing.T)
```

Register a handler that returns a stale error carrying any legacy marker.
Assert DynamicPolicy invokes it exactly once.

- [ ] **Step 2: Run RED**

```bash
MOCKEY_CHECK_GCFLAGS=false go test -race \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestRunCPUSetAdjustmentHandlersDoesNotInterpretTopologyReplanErrors$' \
  -count=1 -v
```

Expected: FAIL because the current generic admission loop retries it.

- [ ] **Step 3: Delete old ownership**

Delete:

```go
cpuSetAdjustmentAdmissionReplans
isFrozenSnapshotDriftReplanSafe
```

Replace the per-handler attempt loop with one handler invocation. Keep
state-generation invalidation at the existing outer transaction boundary.

- [ ] **Step 4: Run focused tests and commit**

```bash
MOCKEY_CHECK_GCFLAGS=false go test -race \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestRunCPUSetAdjustmentHandlersDoesNotInterpretTopologyReplanErrors$' \
  -count=1
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go
git commit -m "refactor(qrm-cpu): make topology plugin the replan owner"
```

---

### Task 7: Prove End-to-End Admission and Post-Advisor Behavior

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`

- [ ] **Step 1: Add complete-path RED tests**

```go
func TestAllocateSucceedsWhenUnrelatedRetiredLeafDisappears(t *testing.T)
func TestAllocateRecompilesWhenOwnedContainerRestartsDuringFinalization(t *testing.T)
func TestRemovePodRetiredLeafDoesNotBlockNextAllocate(t *testing.T)
func TestAdvisorPostCommitRecompilesLifecycleDriftWithoutMarkingApplyFailed(t *testing.T)
func TestAdvisorPostCommitTerminalRollbackFailureRetainsWALAndFence(t *testing.T)
```

The first four must end without `alloc_failed`, without an ApplyFailed retry,
and with one published AppliedView from the latest proof generation. The last
must remain fail closed.

- [ ] **Step 2: Run RED**

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test(Allocate.*Lifecycle|AllocateSucceedsWhenUnrelated|RemovePodRetired|AdvisorPostCommit.*Lifecycle|AdvisorPostCommitTerminal)' \
  -count=1 -v
```

Expected: at least the finalization and recompile cases fail before the
implementation tasks.

- [ ] **Step 3: Make only integration wiring changes exposed by RED**

No new fallback is allowed. Fix missing proof plumbing at the canonical
compiler, validator, coordinator disposition, or plugin recompile owner.

- [ ] **Step 4: Run package verification**

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -count=1
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -count=1
MOCKEY_CHECK_GCFLAGS=false go test -race \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Test(Allocate.*Lifecycle|AdvisorPostCommit.*Lifecycle|RunCPUSetAdjustmentHandlersDoesNotInterpret)' \
  -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go
git commit -m "test(qrm-cpu): cover lifecycle replan transaction"
```

---

## Final Verification

- [ ] Run the complete relevant tree:

```bash
go test -p 1 -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... \
  -count=1
```

- [ ] Run focused race:

```bash
MOCKEY_CHECK_GCFLAGS=false go test -race \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... \
  -run 'LifecycleProof|Replan|RetiredLeaf|ContainerRestarts' \
  -count=1
```

- [ ] Run static gates:

```bash
gofmt -w \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
go vet \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... \
  ./pkg/agent/sysadvisor/plugin/qosaware/server/...
git diff --check
git status --short
```

- [ ] Verify retirement:

```bash
rg 'isFrozenSnapshotDriftReplanSafe|cpuSetAdjustmentAdmissionReplans' \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy
rg 'ResolveContainerRelPathWithContext' \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology
```

Expected:

- the first command returns no production references;
- the second returns build/compiler references only, never finalization;
- the worktree contains only the intended implementation commits.
