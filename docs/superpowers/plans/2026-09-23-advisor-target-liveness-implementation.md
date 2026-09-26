# Advisor Target Liveness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every pending Advisor post-commit target a cancellation-safe path to either complete or hand canonical reconciliation to the latest state, without waiting for an unrelated API call or residual-cleanup tick.

**Architecture:** Keep the WAL target as the durable crash-recovery owner, but add one runtime liveness timer that only wakes the existing serialized CPUSet retry owner. Retry and periodic execution share one canonical decision function: reconcile the current fenced target, release only a threshold-expired physical-apply fence after response-owned effects converge, retire a released target once a newer canonical revision supersedes it, and then reconcile the latest canonical state. Startup classifies active/staging WAL against canonical revision through an explicit matrix; `Stop` cancels and joins both timer and retry work.

**Tech Stack:** Go 1.18, Kubernetes wait/context primitives, QRM CPU canonical state revision and Advisor WAL, testify, Go race detector.

---

## Constraints and Invariants

- Modify production and test files listed below only; do not change protobufs, checkpoint/WAL schemas, health tolerances, cgroup paths, or configuration APIs.
- Preserve the durable active/staging WAL and checksum formats.
- Preserve the execution lease as the only serializer for physical CPUSet work.
- A threshold timer is a wakeup source, never a second reconciliation owner.
- Only retry and periodic paths may perform released-target supersession and latest-canonical reconciliation.
- Admission, `RemovePod`, `GetResourcesAllocation`, and residual cleanup may wait for or consume a released fence, but must not replay, retire, or supersede the target.
- Release is allowed only for an unchanged target in `physical_apply`, after the threshold, and after response-owned migration/headroom/cgroup effects converge.
- A released target and its WAL remain present until the canonical owner either completes that exact revision or observes a newer canonical revision and retires the stale WAL.
- Any unknown, malformed, or impossible recovery tuple fails closed.
- Every production change follows RED → observed failure → minimal GREEN → focused test → atomic commit.

## File Map

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
  to define the timer, released-target state machine, startup classification, and the single retry/periodic canonical owner.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`
  for threshold wakeups, owner parity, supersession, restart tuples, and cancellation.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go`
  to hold liveness-worker state and wire `Start`/`Stop`.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go`
  for API wait semantics, restart/start behavior, and `Stop` joining.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
  to route pre-frame draining through the canonical owner rather than a second target lifecycle path.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
  to prove a new Advisor frame cannot overwrite a fenced target and can proceed after canonical supersession.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler.go`
  to remove residual cleanup as a stuck-target recovery owner.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler_test.go`
  to prove cleanup observes/waits only and resumes after owner-driven release.

## Task 1: Add a Deterministic Threshold Timer

- **Files:**
  - Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go:65-102,1178-1254`
  - Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go:228-240`
  - Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`

- [ ] **Step 1: Write RED tests for threshold scheduling and re-arming**

```go
func TestAdvisorTargetLivenessTimerSchedulesRetryAtThreshold(t *testing.T)
func TestAdvisorTargetLivenessTimerRearmsOnProgress(t *testing.T)
func TestAdvisorTargetLivenessTimerIgnoresNonPhysicalPhase(t *testing.T)
func TestAdvisorTargetLivenessTimerIgnoresReplacedTarget(t *testing.T)
```

Use an injected `now func() time.Time`, `newTimer func(time.Duration) advisorTargetTimer`,
and a buffered retry notification hook. Assert that:

1. entering `physical_apply` arms exactly one timer for
   `advisorPostCommitStuckThreshold(conf) - sinceProgress`;
2. a generation change cancels the old deadline and arms from the new
   `lastProgressAt`;
3. `prepared`, `published`, `applied_marker`, and `cleanup` never arm the
   physical-apply handoff timer;
4. replacing or clearing the target makes a stale timer callback a no-op.

- [ ] **Step 2: Run RED and record the compile failure**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestAdvisorTargetLivenessTimer' -count=1 -v
```

Expected: `FAIL` at compile time because `advisorTargetTimer`,
`runAdvisorTargetLivenessTimer`, and the injected clock/timer fields do not
exist.

- [ ] **Step 3: Add the compile-oriented timer contract**

```go
type advisorTargetTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type realAdvisorTargetTimer struct{ timer *time.Timer }

func (t realAdvisorTargetTimer) C() <-chan time.Time { return t.timer.C }
func (t realAdvisorTargetTimer) Stop() bool          { return t.timer.Stop() }

type advisorTargetIdentity struct {
	target     *advisorPostCommitTarget
	revision   uint64
	generation uint64
	phase      advisorPostCommitPhase
}

func (p *DynamicPolicy) advisorTargetIdentity() (advisorTargetIdentity, <-chan struct{}) {
	progress := p.currentAdvisorPostCommitProgress()
	return advisorTargetIdentity{
		target: progress.target, revision: progress.revision,
		generation: progress.generation, phase: progress.phase,
	}, progress.changed
}
```

Add private `DynamicPolicy` fields:

```go
advisorTargetLivenessWG       sync.WaitGroup
advisorTargetLivenessStopCh   <-chan struct{}
advisorTargetNow              func() time.Time
advisorTargetNewTimer         func(time.Duration) advisorTargetTimer
advisorTargetRetryNotification func(cpusetutil.CPUSetAdjustmentRetryReason)
```

Default the hooks to `time.Now`, `time.NewTimer`, and
`scheduleCPUSetAdjustmentRetry`. Keep all hook access private to this package.
Use adapters so the concrete timer satisfies the interface:

```go
func (p *DynamicPolicy) newAdvisorTargetTimer(delay time.Duration) advisorTargetTimer {
	if p.advisorTargetNewTimer != nil {
		return p.advisorTargetNewTimer(delay)
	}
	return realAdvisorTargetTimer{timer: time.NewTimer(delay)}
}

func (p *DynamicPolicy) notifyAdvisorTargetRetry(reason cpusetutil.CPUSetAdjustmentRetryReason) {
	if p.advisorTargetRetryNotification != nil {
		p.advisorTargetRetryNotification(reason)
		return
	}
	p.scheduleCPUSetAdjustmentRetry(reason)
}
```

- [ ] **Step 4: Implement one cancellable watcher**

```go
func (p *DynamicPolicy) runAdvisorTargetLivenessTimer(stopCh <-chan struct{}) {
	defer p.advisorTargetLivenessWG.Done()
	for {
		id, changed := p.advisorTargetIdentity()
		if id.target == nil || id.phase != advisorPostCommitPhasePhysicalApply {
			select {
			case <-stopCh:
				return
			case <-changed:
				continue
			}
		}

		delay := p.advisorPostCommitThresholdRemaining(id)
		timer := p.newAdvisorTargetTimer(delay)
		select {
		case <-stopCh:
			timer.Stop()
			return
		case <-changed:
			timer.Stop()
		case <-timer.C():
			if p.advisorTargetIdentityStillCurrent(id) {
				p.notifyAdvisorTargetRetry(cpusetutil.RetryReasonApplyFailed)
			}
			select {
			case <-stopCh:
				return
			case <-changed:
			}
		}
	}
}
```

`advisorPostCommitThresholdRemaining` must clamp an already-expired target to
zero. `advisorTargetIdentityStillCurrent` must compare pointer, revision,
generation, and phase while holding `cpuSetAdjustmentRetryMu`; no callback may
act on a stale snapshot. After firing once, the watcher waits for a target
change instead of spinning on a zero-duration timer; bounded retry and the
periodic owner provide subsequent attempts.

- [ ] **Step 5: Run GREEN and race repetitions**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestAdvisorTargetLivenessTimer' -count=100
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestAdvisorTargetLivenessTimer' -count=20
```

Expected: all four tests pass; race output contains no race report; the
re-arm test observes one notification only from the newest generation.

- [ ] **Step 6: Commit the timer contract atomically**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go
git commit -m "feat(qrm-cpu): wake advisor target owner at liveness threshold"
```

## Task 2: Make Retry and Periodic Reconciliation the Canonical Owner

- **Files:**
  - Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go:1256-1670`
  - Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`

- [ ] **Step 1: Write RED owner-parity tests**

```go
func TestCanonicalAdjustmentOwnerRetryReleasesExpiredPhysicalTarget(t *testing.T)
func TestCanonicalAdjustmentOwnerPeriodicReleasesExpiredPhysicalTarget(t *testing.T)
func TestCanonicalAdjustmentOwnerDoesNotReleasePublishedTarget(t *testing.T)
func TestCanonicalAdjustmentOwnerDoesNotReleaseBeforeThreshold(t *testing.T)
func TestCanonicalAdjustmentOwnerRequiresResponseOwnedEffectsToConverge(t *testing.T)
func TestCanonicalAdjustmentOwnerSerializesReleaseWithExecutionLease(t *testing.T)
```

Run each case once with `CPUSetAdjustmentModeRetry` and once with
`CPUSetAdjustmentModePeriodic`. In the failure case, inject an error from
migration transition, headroom, or cgroup apply and assert the target remains
fenced, the WAL remains, and `RetryReasonApplyFailed` remains dirty.

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestCanonicalAdjustmentOwner' -count=1 -v
```

Expected: `FAIL`; the existing retry and periodic call sites choose work
independently and there is no owner result that distinguishes target replay,
fence release, supersession, and latest-state reconciliation.

- [ ] **Step 3: Introduce one compile-oriented owner result and entry point**

```go
type canonicalAdjustmentAction string

const (
	canonicalAdjustmentNoop             canonicalAdjustmentAction = "noop"
	canonicalAdjustmentTarget           canonicalAdjustmentAction = "target"
	canonicalAdjustmentReleasedTarget   canonicalAdjustmentAction = "released_target"
	canonicalAdjustmentSupersededTarget canonicalAdjustmentAction = "superseded_target"
	canonicalAdjustmentLatestState      canonicalAdjustmentAction = "latest_state"
)

type canonicalAdjustmentResult struct {
	action   canonicalAdjustmentAction
	revision uint64
	released bool
	retired  bool
}

func (p *DynamicPolicy) reconcileCanonicalCPUSetAdjustment(
	ctx context.Context,
	mode cpusetutil.CPUSetAdjustmentMode,
) (canonicalAdjustmentResult, error)

func (p *DynamicPolicy) advisorPostCommitProgressStuck(
	progress advisorPostCommitProgress,
) bool {
	now := time.Now()
	if p.advisorTargetNow != nil {
		now = p.advisorTargetNow()
	}
	return progress.target != nil &&
		progress.phase == advisorPostCommitPhasePhysicalApply &&
		now.Sub(progress.lastProgressAt) >= advisorPostCommitStuckThreshold(p.conf)
}
```

This function is the sole decision owner used by
`scheduleCPUSetAdjustmentRetry` and `reconcileDirtyCPUSetAdjustment`. It must
run while the caller holds `p.Lock`, acquire/reuse the execution lease, persist
restored state first, snapshot target identity under
`cpuSetAdjustmentRetryMu`, and choose exactly one action.

- [ ] **Step 4: Implement threshold handoff inside the owner**

```go
if target != nil && !target.writerFenceReleased {
	if !p.advisorPostCommitProgressStuck(progress) {
		err = p.reconcileAdvisorPostCommitTarget(ctx, target, mode)
		return resultForTarget(target), combine(persistErr, err)
	}
	if progress.phase != advisorPostCommitPhasePhysicalApply {
		return resultForTarget(target), combine(persistErr, stuckTargetError(progress))
	}
	if err = p.replayAdvisorResponseOwnedEffects(target); err != nil {
		return resultForTarget(target), combine(persistErr, err)
	}
	if !p.releaseAdvisorPostCommitWriterFenceIfCurrent(progress) {
		return canonicalAdjustmentResult{action: canonicalAdjustmentNoop}, persistErr
	}
	return canonicalAdjustmentResult{
		action: canonicalAdjustmentReleasedTarget,
		revision: target.revision,
		released: true,
	}, persistErr
}
```

The actual implementation should reuse existing migration/headroom/cgroup
helpers and existing error composition rather than introducing a generic
`combine` helper. Do not run CPUSet handlers from a stale response after the
fence is released.

- [ ] **Step 5: Route both call sites through the owner**

```go
_, err := p.reconcileCanonicalCPUSetAdjustment(
	ctx, cpusetutil.CPUSetAdjustmentModeRetry)
```

and:

```go
_, err := p.reconcileCanonicalCPUSetAdjustment(
	ctx, cpusetutil.CPUSetAdjustmentModePeriodic)
```

Delete or reduce `retryLatestCPUSetAdjustment` so it cannot remain a parallel
policy owner. Keep backoff/queue coalescing in the retry worker and scheduling
in the periodical handler; move only lifecycle decisions into the canonical
function.

- [ ] **Step 6: Run focused GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestCanonicalAdjustmentOwner|TestQueuedRetryRequestsAlwaysProduceOneTrailingLatestStateRound|TestCPUSetAdjustmentRetry)' \
  -count=1 -v
```

Expected: owner-parity tests pass; retry backoff still performs at most
`cpuSetAdjustmentRetryMaxAttempts`; periodic reconciliation produces the same
state transition as retry.

- [ ] **Step 7: Commit the owner refactor atomically**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go
git commit -m "refactor(qrm-cpu): centralize advisor target liveness ownership"
```

## Task 3: Implement Released-Target Supersession

- **Files:**
  - Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go:75-92,1302-1528`
  - Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`

- [ ] **Step 1: Write RED tests for exact-revision completion and supersession**

```go
func TestReleasedAdvisorTargetSameRevisionCompletesExactTarget(t *testing.T)
func TestReleasedAdvisorTargetNewerCanonicalRevisionRetiresWithoutStaleReplay(t *testing.T)
func TestReleasedAdvisorTargetCannotRetireBeforeCanonicalRevisionAdvances(t *testing.T)
func TestReleasedAdvisorTargetCleanupFailureRetainsWALAndDirtyReason(t *testing.T)
func TestReleasedAdvisorTargetReplacementCannotRetireNewTarget(t *testing.T)
func TestReleasedAdvisorTargetSupersedeRunsLatestCanonicalCPUSetOnce(t *testing.T)
```

Record calls to response-owned apply and CPUSet handlers. For the superseded
case, release target revision `N`, commit canonical revision `N+1`, and assert:
the revision-`N` response is not replayed, active/staging WAL is removed, the
pointer is cleared only if it is still target `N`, and one periodic/retry-mode
CPUSet round reads revision `N+1`.

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestReleasedAdvisorTarget' -count=1 -v
```

Expected: `FAIL` because `writerFenceReleased` and atomic
retire-if-current/superseded behavior do not exist on this branch.

- [ ] **Step 3: Add the released state and identity-safe helpers**

```go
// Add to advisorPostCommitTarget.
writerFenceReleased bool // runtime only; never serialized

// Add to advisorPostCommitProgress so waiters do not read target fields unlocked.
fenceReleased bool

func (p *DynamicPolicy) releaseAdvisorPostCommitWriterFenceIfCurrent(
	observed advisorPostCommitProgress,
) bool

func (p *DynamicPolicy) advisorPostCommitTargetSuperseded(
	target *advisorPostCommitTarget,
	canonicalRevision uint64,
) bool

func (p *DynamicPolicy) retireSupersededAdvisorPostCommitTarget(
	target *advisorPostCommitTarget,
) error
```

`releaseAdvisorPostCommitWriterFenceIfCurrent` must compare target pointer,
revision, generation, and `physical_apply` phase before deleting only that
target's write permits. It sets `writerFenceReleased`, advances the runtime
phase to `canonical_reconcile`, closes the target-change channel, leaves target
and WAL installed, and leaves `RetryReasonApplyFailed` dirty.

- [ ] **Step 4: Implement the owner decision table**

| Target state | Canonical revision | Owner action |
|---|---:|---|
| fenced | `target.revision` | reconcile exact target |
| released | `target.revision` | run canonical CPUSet convergence; persist applied marker and clean WAL only after success |
| released | `> target.revision` | delete stale WAL, retire target if pointer-identical, then run latest canonical CPUSet convergence |
| any | `< target.preCommitRevision` | fail closed as impossible rollback |
| fenced | `> target.revision` | fail closed; a fenced target must prevent this tuple |

Do not use `!=` as shorthand for supersession. A lower or unrelated revision
is corruption, not a newer owner.

- [ ] **Step 5: Preserve cleanup ordering**

For released same-revision completion:

```text
latest canonical CPUSet apply
  -> applied marker fsync
  -> active/staging WAL removal
  -> pointer/permit retirement
  -> clear ApplyFailed only if no other reason remains
```

For newer-revision supersession:

```text
prove target is released and canonical revision is newer
  -> remove active/staging WAL
  -> retire pointer if still identical
  -> keep dirty
  -> reconcile latest canonical CPUSet
  -> clear dirty only after successful latest-state convergence
```

- [ ] **Step 6: Run GREEN and stale-pointer stress**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestReleasedAdvisorTarget' -count=100
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'ReleasedAdvisorTarget|AdvisorPostCommitProgressIdentity' -count=20
```

Expected: no stale replay, no new-target retirement, exactly one latest-state
round per successful supersession, and no race report.

- [ ] **Step 7: Commit released-target handling atomically**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go
git commit -m "fix(qrm-cpu): supersede released advisor targets safely"
```

## Task 4: Lock the Restart Matrix

- **Files:**
  - Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go:897-1092`
  - Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`
  - Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go`

- [ ] **Step 1: Write a table-driven RED restart matrix**

```go
func TestAdvisorPostCommitRestartMatrix(t *testing.T) {
	tests := []struct {
		name              string
		active            *advisorPostCommitCheckpoint
		staging           *advisorPostCommitCheckpoint
		canonicalRevision uint64
		wantAction        advisorPostCommitRecoveryAction
		wantTarget        bool
		wantDirty         bool
		wantErr           string
	}{
		{"staging before commit", nil, wal(10, 11, false), 10, recoveryDiscardStaging, false, false, ""},
		{"staging committed", nil, wal(10, 11, false), 11, recoveryPromoteAndReplay, true, true, ""},
		{"active committed unapplied", wal(10, 11, false), nil, 11, recoveryReplay, true, true, ""},
		{"active committed applied", wal(10, 11, true), nil, 11, recoveryCleanupApplied, true, true, ""},
		{"duplicate promotion evidence", wal(10, 11, false), wal(10, 11, false), 11, recoveryReplay, true, true, ""},
		{"new staging supersedes old active", wal(9, 10, true), wal(10, 11, false), 11, recoveryPromoteAndReplay, true, true, ""},
		{"released then superseded", wal(10, 11, false), nil, 12, recoveryRetireSuperseded, false, true, ""},
		{"staging released then superseded", nil, wal(10, 11, false), 12, recoveryRetireSuperseded, false, true, ""},
		{"canonical rollback", wal(10, 11, false), nil, 9, recoveryInvalid, false, false, "behind pre-commit revision"},
		{"active and staging conflict", walWithResponse(10, 11, false, "A"), walWithResponse(10, 11, false, "B"), 11, recoveryInvalid, false, false, "conflicting"},
		{"corrupt active", corruptWAL(), nil, 11, recoveryInvalid, false, false, "corrupted active"},
	}
	// Persist fixtures, construct a fresh policy, call Start preparation,
	// and assert target/WAL/dirty/error state for every row.
}
```

The helper names above are test-local. `wal` creates a target with a valid
checksum; `walWithResponse` does the same while encoding its final argument in
the response so two records with equal revisions are observably different;
`corruptWAL` writes invalid JSON. Persist all valid fixtures through production
serialization helpers rather than hand-writing JSON.

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestAdvisorPostCommitRestartMatrix$' -count=1 -v
```

Expected: at least the superseded rows fail because current mismatch cleanup
does not preserve a latest-canonical reconciliation obligation; impossible
lower revisions are not distinguished from harmless stale WAL.

- [ ] **Step 3: Replace the three-state recovery enum with explicit actions**

```go
type advisorPostCommitRecoveryAction uint8

const (
	recoveryInvalid advisorPostCommitRecoveryAction = iota
	recoveryDiscardStaging
	recoveryPromoteAndReplay
	recoveryReplay
	recoveryCleanupApplied
	recoveryRetireSuperseded
)

func classifyAdvisorPostCommitRecovery(
	active, staging *advisorPostCommitTarget,
	canonicalRevision uint64,
) (advisorPostCommitRecoveryAction, *advisorPostCommitTarget, error)
```

The classifier must be pure: no filesystem mutation, target publication, or
dirty-bit updates. Compare `preCommitRevision`, `revision`, applied state, and
active/staging identity. Return a descriptive error for duplicate/conflicting
owners and impossible revision rollback.

- [ ] **Step 4: Execute the classified action with crash-safe ordering**

```go
action, selected, err := classifyAdvisorPostCommitRecovery(active, staging, mainRevision)
if err != nil {
	return err
}
switch action {
case recoveryDiscardStaging:
	return p.removeAdvisorPostCommitStaging()
case recoveryPromoteAndReplay:
	if err := p.promoteAdvisorPostCommitStaging(); err != nil { return err }
	p.publishRecoveredAdvisorTarget(selected)
	p.markAdvisorTargetRetryDirty()
case recoveryReplay, recoveryCleanupApplied:
	if err := p.removeAdvisorPostCommitStaging(); err != nil { return err }
	p.publishRecoveredAdvisorTarget(selected)
	p.markAdvisorTargetRetryDirty()
case recoveryRetireSuperseded:
	if err := p.removeAdvisorPostCommitCheckpoints(); err != nil { return err }
	p.markLatestCanonicalRetryDirty()
default:
	return fmt.Errorf("invalid advisor post-commit recovery action %d", action)
}
```

The compile-oriented draft shows ordering, not permission to duplicate dirty
state logic. Use one locked helper for reason-map updates. Recovered runtime
progress starts with a fresh clock; a released flag is deliberately not
restored because it was never durable.

- [ ] **Step 5: Prove `Start` schedules the selected obligation**

Add:

```go
func TestStartSchedulesRecoveredAdvisorTargetOwner(t *testing.T)
func TestStartSchedulesSupersededWALLatestCanonicalOwner(t *testing.T)
func TestStartFailsClosedBeforeWorkersOnInvalidRestartTuple(t *testing.T)
```

The first two wait for the canonical owner and assert exact-target replay or
latest-state reconciliation respectively. The invalid tuple must return an
error without starting the timer, retry worker, Advisor loop, or periodical
handlers.

- [ ] **Step 6: Run GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestAdvisorPostCommitRestartMatrix|TestStartSchedules.*Owner|TestStartFailsClosedBeforeWorkers)' \
  -count=20 -v
```

Expected: every matrix row reaches its declared action; superseded WAL is never
replayed; invalid rows leave evidence on disk and start no background work.

- [ ] **Step 7: Commit restart classification atomically**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go
git commit -m "fix(qrm-cpu): classify advisor target restart ownership"
```

## Task 5: Remove Secondary Recovery Owners

- **Files:**
  - Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go:1376-1427`
  - Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go:620-634`
  - Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler.go:388-515`
  - Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go`
  - Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
  - Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler_test.go`

- [ ] **Step 1: Write RED tests that forbid request/cleanup ownership**

```go
func TestRemovePodWaiterDoesNotReplayOrRetireAdvisorTarget(t *testing.T)
func TestAllocateWaiterDoesNotReplayOrRetireAdvisorTarget(t *testing.T)
func TestClearResidualStateDoesNotReleaseStuckAdvisorTarget(t *testing.T)
func TestAdvisorFrameUsesCanonicalOwnerBeforePlanningNextRevision(t *testing.T)
func TestAdvisorFrameCannotReplaceUnreleasedTarget(t *testing.T)
```

Block the canonical owner behind a channel, invoke each API/cleanup path, and
assert zero response-owned apply calls, zero WAL removals, and unchanged target
identity until the owner is released. For the Advisor frame, assert planning
starts only after the owner resolves the prior target.

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(Test(RemovePod|Allocate)WaiterDoesNot|TestClearResidualStateDoesNotRelease|TestAdvisorFrame)' \
  -count=1 -v
```

Expected: `FAIL` if `waitForPendingAdvisorPostCommitTargetLocked`,
`clearResidualStateAttempt`, or the Advisor frame directly performs stuck
recovery or target retirement.

- [ ] **Step 3: Reduce API waiters to observation**

```go
func (p *DynamicPolicy) waitForPendingAdvisorPostCommitTargetLocked(
	ctx context.Context,
	source string,
) error {
	for {
		progress := p.currentAdvisorPostCommitProgress()
		target, changed := progress.target, progress.changed
		if target == nil || progress.fenceReleased {
			return nil
		}
		p.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		p.Lock()
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s blocked by pending advisor post-commit revision %d: %w",
				source, target.revision, err)
		}
	}
}
```

After acquiring the execution lease, recheck that the target is either absent
or released before allowing the canonical write. The waiter must never call
response apply, fence release, WAL removal, or target retirement.

- [ ] **Step 4: Make residual cleanup observer-only**

Delete any call from `clearResidualStateAttempt` to stuck-target recovery.
Retain residual aging, bounded target-change wait, writer-permit check, and
revision CAS. Behavior:

- fenced progressing target: defer healthy;
- fenced threshold-expired target: return the existing diagnostic stuck error;
- released target: proceed through the normal writer gate and revision CAS;
- target identity changes during wait: retry once without double-aging.

- [ ] **Step 5: Route Advisor pre-frame work through the same owner**

Replace direct calls to `reconcileAdvisorPostCommitTarget` with:

```go
_, reconcileErr := p.reconcileCanonicalCPUSetAdjustment(
	ctx, cpusetutil.CPUSetAdjustmentModeRetry)
```

The execution lease remains held across prior-target resolution and planning of
the next frame, preventing a second owner or target replacement window.

- [ ] **Step 6: Run focused GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'AdvisorFrame|WaiterDoesNot|ClearResidualState|ExecutionLeaseRechecksAdvisorTarget' \
  -count=50
```

Expected: API waiters and cleanup perform no lifecycle side effects; a released
target allows canonical mutation; the next Advisor frame cannot overwrite an
unreleased owner.

- [ ] **Step 7: Commit ownership cleanup atomically**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler_test.go
git commit -m "refactor(qrm-cpu): keep advisor target recovery owner-only"
```

## Task 6: Make Start and Stop Cancellation Complete

- **Files:**
  - Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go:650-683,866-896`
  - Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go:1543-1634`
  - Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go`
  - Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`

- [ ] **Step 1: Write RED lifecycle tests**

```go
func TestStopCancelsAdvisorTargetThresholdTimer(t *testing.T)
func TestStopCancelsCanonicalOwnerDuringExecutionLeaseWait(t *testing.T)
func TestStopCancelsCanonicalOwnerDuringHandlerExecution(t *testing.T)
func TestStopCancelsCanonicalOwnerDuringRetryBackoff(t *testing.T)
func TestStopWaitsForAdvisorTargetAndRetryWorkers(t *testing.T)
func TestStartFailureCancelsAdvisorTargetAndRetryWorkers(t *testing.T)
func TestStartStopStartUsesFreshLivenessGeneration(t *testing.T)
```

Each test must use channels, contexts, or an injected timer; do not use sleeps
to prove cancellation. Assert `Stop` returns, all wait groups reach zero, no
post-stop retry notification is accepted, and a second `Start` does not reuse a
closed channel or stale timer generation.

- [ ] **Step 2: Run RED**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestStopCancelsAdvisorTarget|TestStopCancelsCanonicalOwner|TestStopWaitsForAdvisor|TestStartFailureCancelsAdvisor|TestStartStopStartUsesFresh)' \
  -count=1 -v
```

Expected: compile failure for the liveness wait group/hooks or timeout/failure
where `Stop` does not yet cancel every wait state.

- [ ] **Step 3: Wire startup only after recovery validation**

```go
if err = p.prepareAdvisorPostCommitTargetOnStart(); err != nil {
	p.cancelStart()
	return fmt.Errorf("prepare pending advisor post-commit target: %w", err)
}

p.cpuSetAdjustmentRetryMu.Lock()
p.cpuSetAdjustmentRetryStopCh = p.stopCh
p.advisorTargetLivenessStopCh = p.stopCh
p.cpuSetAdjustmentRetryStopping = false
p.cpuSetAdjustmentRetryMu.Unlock()

p.advisorTargetLivenessWG.Add(1)
go p.runAdvisorTargetLivenessTimer(p.stopCh)
if p.hasPendingCanonicalAdjustmentWork() {
	p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonApplyFailed)
}
```

If current `Start` must create `stopCh` before recovery I/O, its deferred error
path must close that channel and join any worker that was started. Do not
register periodical handlers or Advisor RPC loops until recovery
classification succeeds.

- [ ] **Step 4: Propagate the stop channel into all owner contexts**

Replace per-attempt bridge goroutines with one helper:

```go
func contextCanceledByStop(parent context.Context, stopCh <-chan struct{}) (
	context.Context, context.CancelFunc,
) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
```

Use it around execution-lease waits and handler execution. If retaining the
helper goroutine, account for it in an existing worker lifetime or prove it
exits through `ctx.Done`; do not add an unjoined per-retry goroutine leak.

- [ ] **Step 5: Enforce stop ordering**

```text
under p.Lock: mark stopped, capture stopCh
under retry mutex: set stopping=true
close stopCh exactly once
release p.Lock
stop periodical handlers so no new periodic owner can enter
wait advisorTargetLivenessWG
wait cpuSetAdjustmentRetryWG
close Advisor connection and remaining components
```

Never hold `p.Lock` or `cpuSetAdjustmentRetryMu` while waiting. Scheduling after
`stopping=true` must be a no-op.

- [ ] **Step 6: Run GREEN and leak/race repetitions**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Stop|StartFailure|StartStopStart|AdvisorTargetLiveness' -count=100
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Stop|StartFailure|StartStopStart|AdvisorTargetLiveness' -count=20
```

Expected: all tests terminate without timeout, no retry occurs after stop, both
wait groups drain, restart uses a fresh stop channel, and race output is clean.

- [ ] **Step 7: Commit lifecycle cancellation atomically**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go
git commit -m "fix(qrm-cpu): cancel advisor liveness workers on stop"
```

## Task 7: Final Verification and Commit Audit

- **Files:**
  - Verify only; no production edits expected.
  - If a test correction is required, modify only the test file that owns the
    failed contract and amend the corresponding atomic commit.

- [ ] **Step 1: Run the complete focused target-liveness suite**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Advisor(PostCommit|Target)|CanonicalAdjustmentOwner|ReleasedAdvisorTarget|ClearResidualState|Start|Stop' \
  -count=20 -v
```

Expected: `PASS`; no test reports stale-response replay, duplicate owner work,
post-stop scheduling, or leaked WAL.

- [ ] **Step 2: Run package tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/state -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
```

Expected: all commands exit `0`.

- [ ] **Step 3: Run race gates**

```bash
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'Advisor(PostCommit|Target)|CanonicalAdjustmentOwner|ReleasedAdvisorTarget|Start|Stop' \
  -count=10
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
```

Expected: all commands exit `0` and emit no `WARNING: DATA RACE`.

- [ ] **Step 4: Run static and formatting checks**

```bash
gofmt -d \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler_test.go
go vet ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
git diff --check
```

Expected: `gofmt -d` and `git diff --check` print nothing; `go vet` exits `0`.

- [ ] **Step 5: Verify there is one lifecycle owner**

```bash
git grep -n 'reconcileAdvisorPostCommitTarget\\|retireSupersededAdvisorPostCommitTarget\\|releaseAdvisorPostCommitWriterFence' \
  -- pkg/agent/qrm-plugins/cpu/dynamicpolicy
```

Expected: production invocations are contained by
`reconcileCanonicalCPUSetAdjustment` and its private helpers; request waiters
and residual cleanup contain no invocation.

- [ ] **Step 6: Verify schema and timeout non-goals**

```bash
git diff --name-only HEAD~6..HEAD
git diff HEAD~6..HEAD -- \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor \
  pkg/agent/qrm-plugins/advisorsvc \
  pkg/config
```

Expected: the first command lists only the exact files in this plan; the second
prints nothing. Confirm `advisorPostCommitCheckpoint` JSON fields, checksum
version, `healthCheckTolerationTimes`, and `stateCheckPeriod` are unchanged.

- [ ] **Step 7: Audit atomic commits**

```bash
git log --oneline --reverse HEAD~6..HEAD
git status --short
```

Expected commit sequence:

```text
feat(qrm-cpu): wake advisor target owner at liveness threshold
refactor(qrm-cpu): centralize advisor target liveness ownership
fix(qrm-cpu): supersede released advisor targets safely
fix(qrm-cpu): classify advisor target restart ownership
refactor(qrm-cpu): keep advisor target recovery owner-only
fix(qrm-cpu): cancel advisor liveness workers on stop
```

If the plan document is committed with the implementation, use a separate
docs-only commit:

```bash
git add docs/superpowers/plans/2026-09-23-advisor-target-liveness-implementation.md
git commit -m "docs(qrm-cpu): plan advisor target liveness ownership"
```

Expected final status: clean except for explicitly pre-existing unrelated
untracked files.

## Acceptance Matrix

| Requirement | Required evidence |
|---|---|
| Threshold timer | exact-deadline, re-arm, stale-callback, phase-filter, and stop tests |
| Retry/periodic canonical owner | parity tests call one decision function under the execution lease |
| Released-target supersede | newer canonical revision retires stale WAL without stale response replay and runs latest state once |
| Same-revision released target | completes physical convergence, applied marker, WAL cleanup, and retry-state cleanup in order |
| Restart matrix | active/staging × before/current/newer/applied/corrupt table is exhaustive and fail-closed |
| Request and cleanup boundaries | Allocate/RemovePod/GetResourcesAllocation/cleanup never replay or retire targets |
| Stop cancellation | timer, lease wait, handler execution, and backoff all cancel and join |
| Crash durability | no WAL schema/checksum change; runtime release marker is not persisted |
| Existing safety | writer permits, canonical revision CAS, execution lease, rollback, and health tolerance remain intact |
