# Post-E2E Systematic Review Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify and harden the changes since `08e96b3f` without weakening fail-closed topology semantics proven by the completed E2E suites.

**Architecture:** Keep the current ownership boundaries: the cpuset topology plugin owns pending Pod scope resolution, the topology compiler owns frozen trace validation, and dynamic policy state revision owns adjustment snapshot freshness. Remove duplicate work and fake optional abstractions inside those boundaries instead of adding compatibility paths.

**Tech Stack:** Go 1.18, Kubernetes Pod/QoS APIs, Linux cgroup v1/v2, QRM dynamic policy, race detector, native linux/amd64 CGO, canonical bulkhead E2E.

## Reviewed State on 2026-09-20

- Pending protection is resolved once per Pod. All pending containers consume
  one fresh Pod/QoS/scope result, and their CPU sets remain individually
  represented while contributing to one protected union.
- A live Pod with no materialized cgroup candidate remains pending. A checkpoint
  allocation is stale only when the strict fresh Pod lookup reports absence and
  every allowed candidate reports typed `ENOENT`; any other observation error is
  preserved and fails closed.
- QoS-filtered candidate generation covers configured cgroupfs and systemd
  roots. Tests isolate process-global cgroup-root initialization in a subprocess,
  while lower-level cgroup tests inject mount and path probes per test so they
  remain parallel-safe.
- ParentSafe admission now validates and freezes one immutable trace at the
  compiler boundary. Reservation, preflight, and execution consume that carrier,
  and projection plus reservation propagate cancellation.
- Allocation paths derive the shared NUMA-binding ratio and hard-partition
  reclaim floor from one cloned `advisorAttemptConfiguration`. A configuration
  update can affect the next allocation attempt, but cannot split these coupled
  decisions within the current attempt.
- Native Linux build and complete E2E validation passed for commit
  `15a0a74806cb6c3d240f84e39f7e5c344a41ab55`. The run completed standard,
  high-churn, overlap-churn, and final-reset phases with zero final failures.

---

### Task 1: Resolve Pending Scope Once Per Pod

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/pending_fresh_scope_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`

- [x] Add a RED test where a fresh live Pod has multiple valid QoS candidates and none is materialized; assert the allocation is not classified stale.
- [x] Add a RED test with two pending containers in one Pod; assert strict fresh Pod/scope resolution runs once and protects the union of both allocations.
- [x] Refactor `buildExpectedCPUSetByRel` into container classification followed by Pod-level pending resolution.
- [x] Make stale classification depend on both fresh Pod absence and absence of every allowed cgroup candidate.
- [x] Remove the unused `Expected` field and document the Pod-level consistency invariant on the resolver.
- [x] Run focused tests, full cpusettopology tests, and race tests.
- [x] Commit as `fix(qrm-cpu): resolve pending protection once per pod` (`ba2b5770`).

### Task 2: Freeze Each Admission Trace Once

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_ticket.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
- Modify: related topology tests and benchmarks

- [x] Add instrumentation tests proving the ParentSafe admission path freezes and validates one trace exactly once.
- [x] Introduce an unexported validated immutable trace carrier produced only by the compiler boundary.
- [x] Let reservation, preflight, and execution consume the validated carrier without cloning or replay-validating it again.
- [x] Keep exported defensive entry points for callers that still provide mutable `CompiledPhaseTrace`.
- [x] Add context checks around expensive projection passes to bound deadline overshoot.
- [x] Remove `phaseTracePhysicalWriteCost`, `equalStringSlices`, and production-only test probes that have no runtime reader when tests can observe returned results directly.
- [x] Run topology focused tests, benchmarks, race tests, and full package tests.
- [x] Commit as `perf(qrm-cpu): freeze admission traces once` (`e1549528`), with validated-boundary and cancellation follow-ups `b95dfec5`, `1d6a4e7d`, and `929cdb16`.

### Task 3: Remove Fake Revision Capability

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`

- [x] Add or update tests proving snapshot matching is revision-only and revision changes invalidate a snapshot.
- [x] Read `GetRevision()` directly from `state.ReadonlyState`.
- [x] Remove `cpuSetAdjustmentRevisionedState`, `hasRevision`, the unreachable deep-comparison fallback, and the `reflect` dependency if unused.
- [x] Add function comments explaining why revision is the canonical freshness owner.
- [x] Run dynamicpolicy focused, race, full package, and `go vet` checks.
- [x] Commit as `refactor(qrm-cpu): use canonical state revision` (`7af6f856`).

### Task 4: Documentation and Final Review

**Files:**
- Modify: design and plan documents affected by the reviewed implementation
- Create: final review report under `qrm-bulkhead-test-artifacts`

- [x] Ensure core ownership, fail-closed rules, and lifecycle invariants are present in function comments.
- [x] Remove documentation claims that no longer match the simplified implementation.
- [x] Keep all documentation changes in one standalone commit.
- [x] Run lingering-reference, `gofmt`, `go vet`, focused repetition, race, and package gates.
- [x] Run native Linux tests and build the exact clean HEAD.
- [x] Run focused high-churn/overlap and three canonical E2E suites, always performing final reset.
- [x] Generate Markdown and HTML review reports with final findings and commit boundaries.
- [x] Commit docs as `docs(qrm-cpu): align reviewed topology invariants`.
