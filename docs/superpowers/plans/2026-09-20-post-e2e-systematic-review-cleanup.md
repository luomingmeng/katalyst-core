# Post-E2E Systematic Review Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify and harden the changes since `08e96b3f` without weakening fail-closed topology semantics proven by the completed E2E suites.

**Architecture:** Keep the current ownership boundaries: the cpuset topology plugin owns pending Pod scope resolution, the topology compiler owns frozen trace validation, and dynamic policy state revision owns adjustment snapshot freshness. Remove duplicate work and fake optional abstractions inside those boundaries instead of adding compatibility paths.

**Tech Stack:** Go 1.18, Kubernetes Pod/QoS APIs, Linux cgroup v1/v2, QRM dynamic policy, race detector, native linux/amd64 CGO, canonical bulkhead E2E.

---

### Task 1: Resolve Pending Scope Once Per Pod

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/pending_fresh_scope_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`

- [ ] Add a RED test where a fresh live Pod has multiple valid QoS candidates and none is materialized; assert the allocation is not classified stale.
- [ ] Add a RED test with two pending containers in one Pod; assert strict fresh Pod/scope resolution runs once and protects the union of both allocations.
- [ ] Refactor `buildExpectedCPUSetByRel` into container classification followed by Pod-level pending resolution.
- [ ] Make stale classification depend on both fresh Pod absence and absence of every allowed cgroup candidate.
- [ ] Remove the unused `Expected` field and document the Pod-level consistency invariant on the resolver.
- [ ] Run focused tests, full cpusettopology tests, and race tests.
- [ ] Commit as `fix(qrm-cpu): resolve pending protection once per pod`.

### Task 2: Freeze Each Admission Trace Once

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_ticket.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/phase_trace_execution.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
- Modify: related topology tests and benchmarks

- [ ] Add instrumentation tests proving the ParentSafe admission path freezes and validates one trace exactly once.
- [ ] Introduce an unexported validated immutable trace carrier produced only by the compiler boundary.
- [ ] Let reservation, preflight, and execution consume the validated carrier without cloning or replay-validating it again.
- [ ] Keep exported defensive entry points for callers that still provide mutable `CompiledPhaseTrace`.
- [ ] Add context checks around expensive projection passes to bound deadline overshoot.
- [ ] Remove `phaseTracePhysicalWriteCost`, `equalStringSlices`, and production-only test probes that have no runtime reader when tests can observe returned results directly.
- [ ] Run topology focused tests, benchmarks, race tests, and full package tests.
- [ ] Commit as `perf(qrm-cpu): freeze admission traces once`.

### Task 3: Remove Fake Revision Capability

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`

- [ ] Add or update tests proving snapshot matching is revision-only and revision changes invalidate a snapshot.
- [ ] Read `GetRevision()` directly from `state.ReadonlyState`.
- [ ] Remove `cpuSetAdjustmentRevisionedState`, `hasRevision`, the unreachable deep-comparison fallback, and the `reflect` dependency if unused.
- [ ] Add function comments explaining why revision is the canonical freshness owner.
- [ ] Run dynamicpolicy focused, race, full package, and `go vet` checks.
- [ ] Commit as `refactor(qrm-cpu): use canonical state revision`.

### Task 4: Documentation and Final Review

**Files:**
- Modify: design and plan documents affected by the reviewed implementation
- Create: final review report under `qrm-bulkhead-test-artifacts`

- [ ] Ensure core ownership, fail-closed rules, and lifecycle invariants are present in function comments.
- [ ] Remove documentation claims that no longer match the simplified implementation.
- [ ] Keep all documentation changes in one standalone commit.
- [ ] Run lingering-reference, `gofmt`, `go vet`, focused repetition, race, and package gates.
- [ ] Run native Linux tests and build the exact clean HEAD.
- [ ] Run focused high-churn/overlap and three canonical E2E suites, always performing final reset.
- [ ] Generate Markdown and HTML review reports with final findings and commit boundaries.
- [ ] Commit docs as `docs(qrm-cpu): align reviewed topology invariants`.
