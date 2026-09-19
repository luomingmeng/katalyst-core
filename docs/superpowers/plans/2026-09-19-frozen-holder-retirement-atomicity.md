# Frozen Holder Retirement Atomicity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make frozen-boundary holder retirement identity-pinned and subtree-atomic across every snapshot scan window without weakening fail-closed behavior.

**Architecture:** The frozen compiler remains the semantic owner and authorizes only non-controlled paths with exact identities. The filesystem driver tolerates a child that vanishes during enumeration, while `snapshotBuilder` distinguishes parent disappearance from child churn, retires complete subtrees, and validates the final evidence graph. The existing post-list identity check moves after recursion, preserving the five-call stable-node I/O bound.

**Tech Stack:** Go, Linux file-descriptor-based cgroup hierarchy driver, Katalyst QRM topology coordinator, testify.

---

## Constraints

- Work only in the dedicated `fake-numa-reclaim-balance` worktree.
- Use RED-first development for every behavior change.
- Do not add retries, sleeps, timeout increases, Pod API lookups, or
  caller-side error suppression.
- Do not change public coordinator or `HierarchyDriver` interfaces.
- Preserve preflight zero-write behavior and full-prefix rollback.
- Preserve the existing stable expanded-node hierarchy-I/O upper bound.
- Keep production comments and documents in English.

## File Map

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary.go`
  to exclude controlled relations from retirement authorization.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go`
  to implement strict absence proof, subtree retirement, and the
  recursion-completion fence.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/cgroup_driver.go`
  to tolerate child disappearance during stable parent enumeration.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/fake_driver_test.go`
  so fake listing requires the parent to exist and can inject exact operation
  failures.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary_test.go`
  for the retirement-window matrix.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot_test.go`
  for structural closure and strict negative cases.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/driver_test.go`
  for real FD enumeration races.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go`
  for final-proof success and rollback behavior.

## Task 1: Expose the True RED

- [ ] Replace the misleading `HierarchyOperationList` retirement fixture. Its
  current implementation deletes on the second stat and never returns an error
  from `ListChildren`.

- [ ] Add an exact list-window test:

```go
func TestEvaluateFrozenBoundaryAllowsAuthorizedHolderRetirementDuringList(t *testing.T)
```

The hook deletes the holder subtree and returns `syscall.ENOENT` when
`HierarchyOperationList` is called for that holder.

- [ ] Add exact post-list and recursive-window tests:

```go
func TestEvaluateFrozenBoundaryAllowsAuthorizedHolderRetirementAtRecursiveFence(t *testing.T)
func TestEvaluateFrozenBoundaryDiscardsAuthorizedParentRetiredDuringChildScan(t *testing.T)
```

- [ ] In every test, assert absence from `Entries`, `Children`,
  `DomainByRel`, `UnavailableChildren`, `ExpandedRels`, and `DomainUnion`.

- [ ] Run RED:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'AuthorizedHolderRetirementDuringList|RetirementAtRecursiveFence|AuthorizedParentRetiredDuringChildScan' \
  -count=1 -v
```

Expected: list and recursive-window cases fail on the current implementation.

- [ ] Commit tests only:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/fake_driver_test.go
git commit -m "test(qrm-cpu): expose frozen holder retirement windows"
```

## Task 2: Tighten Absence Semantics

- [ ] Add RED tests proving that retirement rejects:

```go
ErrCgroupControllerUnavailable
syscall.EACCES
syscall.EIO
context.DeadlineExceeded
ErrHierarchyIOOperationBudgetExceeded
```

- [ ] Add a test proving that the text-only error
  `"read cpuset.cpus: no such file or directory"` is not sufficient retirement
  evidence.

- [ ] Introduce a private typed predicate for cgroup path absence. It must use
  `errors.Is` and explicitly reject `ErrCgroupControllerUnavailable`; it must
  not inspect error strings.

- [ ] Replace retirement-only calls to `isCgroupNotFoundError` with the strict
  predicate. Do not change the broad helper used by unrelated compatibility
  paths in this task.

- [ ] Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Retirement.*(Controller|Permission|IO|Deadline|Budget|Text)' \
  -count=1 -v
```

- [ ] Commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot_test.go
git commit -m "fix(qrm-cpu): require typed holder retirement absence"
```

## Task 3: Constrain Authorization

- [ ] Add:

```go
func TestFrozenBoundaryRetirementAuthorizationExcludesControlledRels(t *testing.T)
func TestFrozenBoundaryRetirementAuthorizationExcludesSharedRequiredPath(t *testing.T)
```

- [ ] Update `frozenBoundaryRetirementAuthorizations` to remove every
  controlled relation after path intersection is resolved.

- [ ] Add validation rejecting duplicate retirable holders and any retirable
  holder that is controlled or not relevant.

- [ ] Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'FrozenBoundary.*(Authorization|Retirable)' -count=1 -v
```

- [ ] Commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary_test.go
git commit -m "fix(qrm-cpu): constrain frozen retirement authorization"
```

## Task 4: Stabilize Child Enumeration

- [ ] Add a real-driver RED test that removes one child between directory
  enumeration and `openChildDirWithIdentity`.

```go
func TestCgroupFSDriverListChildrenSkipsVanishedChild(t *testing.T)
```

- [ ] Assert that surviving children are returned with stable identities.

- [ ] Add negative siblings for permission, cross-device, symlink, and identity
  errors; none may be skipped.

- [ ] Change `listChildrenWithBudget` to continue only for a typed path-absence
  error from opening the named child. Preserve context and node-budget checks.

- [ ] Make `fakeHierarchyDriver.ListChildren` return `syscall.ENOENT` when the
  requested parent does not exist. This aligns fake and production semantics.

- [ ] Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'ListChildren.*(VanishedChild|Permission|CrossDevice|Symlink|Identity)' \
  -count=1 -v
```

- [ ] Commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/cgroup_driver.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/driver_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/fake_driver_test.go
git commit -m "fix(qrm-cpu): tolerate vanished listed children"
```

## Task 5: Make Retirement Subtree-Atomic

- [ ] Change `snapshotBuilder.scan` to return `(bool, error)`, where the boolean
  means the exact requested subtree retired.

- [ ] Add `authorizedRetirement` and `retireSubtree` helpers.

- [ ] Remove incremental `DomainUnion` updates from `scan`; rebuild the union
  from committed entries before fingerprinting.

- [ ] Handle `ListChildren` path absence by confirming the requested parent:

```text
confirmed absent -> retire
same identity -> return original list error
different identity -> identity-changed error
other stat error -> fail closed
```

- [ ] Move the existing post-list path stat after child recursion. If it
  confirms authorized absence, remove the complete provisional subtree. If it
  observes another identity, fail stale.

- [ ] Keep parent child membership only when the child scan returns
  `retired=false`.

- [ ] Run the Task 1 RED tests and require GREEN.

- [ ] Add negative matrix tests for non-retirable, semantic, controlled, and
  replacement paths at list and recursive fences.

- [ ] Commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/frozen_boundary_test.go
git commit -m "fix(qrm-cpu): retire frozen holder subtrees atomically"
```

## Task 6: Enforce Snapshot Closure

- [ ] Add RED tests for dangling child references, identity mismatches,
  duplicate child names, expanded relations without entries, and stale domain
  unions.

- [ ] Extend `validateCompleteSnapshotEvidence` to reject each malformed shape.

- [ ] Call structural validation before a successful snapshot is fingerprinted
  and returned.

- [ ] Assert that validation never mutates or repairs the snapshot.

- [ ] Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'ValidateCompleteSnapshotEvidence|Snapshot.*Closure' -count=1 -v
```

- [ ] Commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot_test.go
git commit -m "fix(qrm-cpu): validate complete snapshot closure"
```

## Task 7: Prove Execution Semantics

- [ ] Add an `executeFrozenTrace` integration test where a non-semantic holder
  retires during final proof after a physical write. Require:

```text
execution succeeds
FinalSnapshotCurrent=true
retired subtree absent
rollback is not invoked
```

- [ ] Add the negative twin for a required holder. Require:

```text
execution fails
the complete write prefix is rolled back
FinalSnapshotCurrent=false
```

- [ ] Add a preflight retirement test and assert physical write count remains
  zero.

- [ ] Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'FrozenTrace.*HolderRetirement|Preflight.*Retirement' -count=1 -v
```

- [ ] Commit:

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution_test.go
git commit -m "test(qrm-cpu): prove frozen retirement execution semantics"
```

## Task 8: Local Verification

- [ ] Run focused repetition:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'Retirement|RetiredDuring|VanishedChild|SnapshotClosure' \
  -count=100
```

- [ ] Run race coverage:

```bash
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  -run 'TestSnapshot|TestEvaluateFrozenBoundary|TestFrozenTrace' \
  -count=10
```

- [ ] Run package and policy regressions:

```bash
go test \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  -count=1

go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
```

- [ ] Run static checks:

```bash
gofmt -d pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology
go vet ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/...
git diff --check
```

- [ ] Confirm stable expanded-node tests still report five logical hierarchy
  operations. Do not increase the automatic budget multiplier.

- [ ] Commit any test-only corrections separately.

## Task 9: Native Linux Verification

- [ ] Build on the dedicated Linux/amd64 CGO build host.
- [ ] Run the native filesystem race tests against the produced source state.
- [ ] Record source commit, build command, binary SHA-256, and module evidence.
- [ ] Deploy the exact binary to QRM and SysAdvisor.
- [ ] Verify runtime and rootfs binary SHA-256 equality before workloads.

Stop if the source identity, runtime identity, or native test evidence does not
match.

## Task 10: E2E Gate

- [ ] Run the focused overlap A-to-B deletion/recreation scenario first.
- [ ] Require no authorized-retirement `complete snapshot list/stat ... no such
  file or directory` admission failure.
- [ ] Require replacement identity and required-holder negative probes to remain
  fail-closed.
- [ ] Run three independent canonical suites. Each suite must execute:

```text
reset dry-run
reset actual
target dry-run
target actual
standard 3 rounds
high churn 5 rounds
overlap churn 3 rounds
final reset
```

- [ ] Require every phase and `FULL_E2E_DONE` to return `rc=0`; final reset is
  mandatory even after failure.
- [ ] Reject any run containing final overlap, failed workload Pod, topology
  budget exhaustion, panic, uncertain rollback, or owned-pool capacity failure.
- [ ] Transfer every archive back through the jump host, verify remote/local
  SHA-256 equality, and run `tar -tzf`.
- [ ] Produce a final report that records all run tags, source and binary
  identities, phase outcomes, hashes, and final node state.

## Completion Criteria

The work is complete only when:

- the true list-window RED fails on the old implementation and passes on the
  new implementation;
- every authorized disappearance window is accepted without ghost evidence;
- every unauthorized, ambiguous, replacement, controller, permission, I/O,
  deadline, and budget case remains fail-closed;
- preflight stays write-free and final-proof rollback semantics remain intact;
- the stable-node hierarchy-I/O bound remains unchanged;
- local, race, native Linux, focused overlap, and three-run canonical gates all
  pass;
- final reset and artifact integrity checks pass.
