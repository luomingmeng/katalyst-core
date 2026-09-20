# Advisor Post-Commit Cleanup Liveness Design

## Status

Approved for implementation.

## Problem

The CPU Advisor loop runs every five seconds and normally returns control
payloads in `ExtraEntries`. A non-empty list prevents the converged no-op path,
so each response may create a new canonical revision and a post-commit target.

`clearResidualState` runs every thirty seconds. The cpuset adjustment runner
releases the policy lock while applying physical changes, but deliberately
keeps the post-commit target published as a writer fence. The cleanup handler
can therefore acquire the policy lock during a progressing physical apply,
observe the target, return a pending error, and report `NotReady`.

The two periods are phase-aligned. Every cleanup heartbeat can hit a different,
successfully progressing target. The revision advances, but the cleanup health
state never returns to `Ready`; after the ninety-second tolerance expires,
healthz returns HTTP 500.

High churn also exposes stale state entries whose Pod and cgroup are already
absent. When their native QoS is empty, pending protection enumerates all
hypothetical cgroup paths and fails closed because several candidates have the
same controlled ancestor depth. That failure blocks `RemovePod`, retains stale
state, and causes later GetAdvice meta-cache failures.

## Goals

- Preserve the post-commit writer fence and fail-closed state ownership.
- Distinguish progressing post-commit work from a stuck transaction.
- Prevent residual detection from starving behind a sequence of short-lived
  targets.
- Retire provably stale pending allocations without guessing a QoS path.
- Preserve crash recovery, WAL durability, revision fencing, and rollback.

## Non-Goals

- Do not increase E2E timeouts or health tolerance.
- Do not remove the writer fence.
- Do not select an arbitrary cgroup candidate.
- Do not make SysAdvisor silently ignore arbitrary missing Pods.
- Do not weaken topology, ownership, or checkpoint validation.
- Do not reduce Advisor revision churn or deduplicate unchanged
  `ExtraEntries`. That is a performance concern owned by a separate design.
- Do not add transaction identity or change checkpoint/WAL schemas.

## Design

### Post-Commit Progress Identity

Each in-memory `advisorPostCommitTarget` records:

- target revision;
- lifecycle phase;
- creation time;
- last progress time;
- monotonically increasing progress generation.

Progress is recorded when the target moves through prepared, published,
physical apply, applied-marker, and cleanup phases. A target replacement also
changes identity.

The progress fields are diagnostic runtime state. They do not change the WAL
schema or recovery authority. A recovered target initializes its progress
clock at recovery and remains subject to the same stuck-target policy.

### Residual Cleanup Deferral

`clearResidualState` separates observation from mutation:

1. Fetch the active Pod list.
2. Acquire the policy lock and inspect the current target progress snapshot.
3. If no target exists, age residual entries and commit eligible deletion as
   today.
4. If a target is progressing:
   - age residual observations without mutating canonical CPU state;
   - release the policy lock;
   - wait, within a bounded fraction of the handler interval, for the target
     change notification;
   - retry once in the same handler invocation.
5. If the target clears, perform the normal revision-fenced deletion attempt.
6. If the same target and progress generation remain unchanged beyond the
   stuck threshold, return an error and report `NotReady`.
7. If the target changed or made progress but remains present, defer mutation
   without reporting health failure.

Residual hit aging is idempotent per handler invocation. A retry in the same
invocation must not increment the same Pod twice.

The stuck threshold is tied to the existing adjustment timeout and must be
strictly shorter than the health tolerance. It is not a new arbitrary E2E
timeout.

### Stale Pending Protection

Pending protection performs one strict fresh Pod lookup for every pending
container path error, including identity-change errors.

- If the Pod exists, derive native QoS from the fresh object.
- If the Pod does not exist, inspect all supported Pod cgroup candidates.
- If no candidate exists, classify the allocation as stale and exclude it from
  `PendingByPod`.
- If exactly one candidate exists, use that concrete path.
- If multiple candidates exist, fail closed.
- Filesystem permission, I/O, cross-device, and identity errors remain fatal.

Scope resolution with unknown QoS may use only the concrete path established
above. It must not infer identity from all hypothetical paths sharing a common
controlled ancestor.

## Failure Semantics

- A target with no progress beyond the threshold remains health-failing.
- WAL publication, applied-marker, cleanup, or directory-fsync failures remain
  health-failing and retryable.
- Residual deletion remains revision-CAS protected.
- Unknown or ambiguous live cgroup scope remains fail-closed.
- Absence is accepted only after both fresh Pod lookup and all cgroup
  candidates prove absence.
- Existing Advisor transaction, checkpoint, and WAL behavior remains unchanged.

## Observability

Pending errors include:

```text
target revision
phase
progress generation
target age
time since progress
```

Metrics distinguish:

- progressing deferral;
- bounded wait recovery;
- stuck target;
- stale pending allocation skipped;
- ambiguous live scope;

## Tests

### Unit

- A progressing target overlapping every cleanup tick never makes health
  unhealthy.
- The same target with no progress becomes unhealthy before health tolerance.
- Residual hit aging continues during progressing deferral and is not doubled
  by same-invocation retry.
- Target change wakes cleanup and permits revision-fenced deletion.
- Missing Pod plus no cgroup candidates is skipped as stale.
- Missing Pod plus one concrete cgroup path is protected using that path.
- Multiple existing paths fail closed.
- Permission and I/O errors fail closed.

### Integration

- `RemovePod` is not globally blocked by an unrelated stale allocation.
- GetAdvice can converge after high-churn deletion without stale meta-cache
  input.
- Existing writer-fence, crash-recovery, and rollback tests remain green.

### Real Node

1. Native Linux race tests.
2. Deploy a source- and SHA-bound CGO binary.
3. Focused high-churn with at least five rounds.
4. Focused overlap with at least three rounds.
5. Three canonical full suites.
6. Fail closed on the first suite failure and always run final reset.

## Acceptance Criteria

- No healthz 500 caused solely by a changing, successfully progressing target.
- No `clearResidualState` starvation across five minutes of continuous
  successful Advisor activity.
- No ambiguous pending scope for a Pod proven absent from both API and cgroup.
- All existing fail-closed tests continue to pass.
- Three independent canonical suites pass with final reset.
