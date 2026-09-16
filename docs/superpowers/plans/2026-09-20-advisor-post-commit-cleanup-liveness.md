# Advisor Post-Commit Cleanup Liveness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent progressing Advisor transactions from starving residual cleanup and retire provably stale pending scopes.

**Architecture:** Add target progress state and bounded target-change retry to cleanup, while retaining the writer fence for mutation. Resolve unknown-QoS pending scopes only from fresh Pod data or concrete existing cgroups. Advisor revision churn remains unchanged and is explicitly outside this repair.

**Tech Stack:** Go 1.18, Kubernetes Pod/QoS API, QRM state checkpoint/WAL, Linux cgroup v1/v2, testify, race detector, native linux/amd64 CGO.

---

## Task 1: Progress Contract

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`

- [ ] Add RED tests for phase/generation/time snapshots, target replacement, notification wakeup, and recovery initialization.
- [ ] Run:
  ```bash
  go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
    -run 'TestAdvisorPostCommitProgress|TestRecoveredAdvisorPostCommitTargetStartsFreshProgressClock' -count=1 -v
  ```
- [ ] Add private `prepared`, `published`, `physical_apply`, `applied_marker`, and `cleanup` phases plus creation/last-progress clocks and a generation.
- [ ] Record progress at every lifecycle boundary without changing WAL authority.
- [ ] Re-run the focused tests and commit.

## Task 2: Cleanup Liveness

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler_test.go`

- [ ] Add RED tests for progressing targets, stuck targets, target replacement, bounded wakeup, single-invocation aging, and no mutation while fenced.
- [ ] Run:
  ```bash
  go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
    -run 'TestClearResidualState(Progressing|Unchanged|Ages|TargetChange|NeverMutates)' -count=1 -v
  ```
- [ ] Split residual observation from mutation.
- [ ] Age each Pod at most once per invocation.
- [ ] On a progressing target, release the lock, wait once on target progress/change, and retry once.
- [ ] Return health error only when the same target and generation exceed an adjustment-timeout-derived threshold below the health tolerance.
- [ ] Keep `ensureCPUStateWriterAllowed` as the final mutation gate.
- [ ] Re-run focused and existing residual cleanup tests and commit.

## Task 3: Stale Pending Scope

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`

- [ ] Add RED tests for identity-change plus missing Pod, zero/one/multiple existing candidates, live Pod QoS recovery, and permission/I/O failures.
- [ ] Run:
  ```bash
  go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
    -run 'Pending|Stale|Fresh|IdentityChange|Scope' -count=1 -v
  ```
- [ ] Perform strict fresh Pod lookup for every pending container error.
- [ ] If Pod is absent, inspect every supported candidate: zero means stale skip, one means concrete scope, multiple means fail closed.
- [ ] Preserve non-absence errors and reject unsafe paths.
- [ ] Use the concrete scope directly when QoS remains unknown.
- [ ] Re-run tests and commit.

## Task 4: Observability and Integration

**Files:**
- Modify: `pkg/agent/qrm-plugins/util/consts.go`
- Test: dynamic policy and cpuset topology integration tests

- [ ] Add result metrics for progressing deferral, wait recovery, stuck target, stale skip, and ambiguous scope.
- [ ] Include revision, phase, generation, target age, and since-progress in stuck diagnostics.
- [ ] Add an integration test proving an unrelated stale allocation no longer blocks `RemovePod`.
- [ ] Add an integration test proving continuously progressing Advisor traffic does not make cleanup health unhealthy.
- [ ] Run focused repetitions and commit.

## Task 5: Local Verification

- [ ] Run focused tests with `-count=100`.
- [ ] Run dynamic policy, topology plugin, and state race suites.
- [ ] Run package regressions:
  ```bash
  go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/state -count=1
  go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology -count=1
  go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology -count=1
  go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
  ```
- [ ] Run `go vet`, `gofmt -d`, and `git diff --check`.
- [ ] Confirm no timeout, health tolerance, writer fence, revision CAS, or checksum weakening.

## Task 6: Native Build and Deployment

- [ ] Archive the exact clean core HEAD and verify its SHA on the Linux builder.
- [ ] Run Linux Go 1.18 focused and race tests.
- [ ] Build a native linux/amd64 CGO agent.
- [ ] Verify ELF, cgo symbols, `libpci.so.3`, `libc.so.6`, size, and SHA-256.
- [ ] Transfer through the HL jump host with SHA verification on every hop.
- [ ] Back up and replace both QRM and SysAdvisor binaries.
- [ ] Verify build, rootfs, and `/proc/<pid>/exe` SHA identity plus healthz.

## Task 7: E2E Gates

- [ ] Run reset dry-run/actual and target dry-run/actual.
- [ ] Run focused high-churn for five rounds.
- [ ] Run focused overlap for three rounds.
- [ ] Verify health remains ready across multiple cleanup ticks, residual state drains, and stale UIDs do not survive as terminal failures.
- [ ] Run three independent canonical suites serially.
- [ ] Stop after the first suite failure, but always execute final reset.
- [ ] Package every run, verify remote/local size and SHA-256, and validate each tar archive.
- [ ] Produce a final report containing source HEAD, binary SHA, run tags, phase results, revision/progress evidence, and final node reset state.
