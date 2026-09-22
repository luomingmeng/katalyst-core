# Stuck Target Review Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the post-commit recovery and SysAdvisor synchronization defects found by the final systematic review without adding a second owner for canonical state.

**Architecture:** Keep canonical CPU state as the sole desired-state authority. A stuck `physical_apply` target may release its writer fence, but it must transfer convergence responsibility to the existing dirty-adjustment loop; successful cleanup alone may retire that responsibility. SysAdvisor derives every QoS-dependent field from one effective QoS value and treats confirmed `PodNotFound` entries as stale cache members.

**Tech Stack:** Go 1.18, QRM advisor WAL/state, SysAdvisor meta cache, Kubernetes Pod API, testify.

---

### Task 1: SysAdvisor QoS and stale-cache consistency

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server.go`
- Test: `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server_test.go`

- [ ] Add a stale-container fixture before `updateMetaCacheInput` and assert that confirmed `PodNotFound` removes it while preserving a live container.
- [ ] Extend the Pod-overrides-metadata test to assert `OriginOwnerPoolName` is derived from the final Pod QoS.
- [ ] Run the focused tests and verify both fail for the intended reasons.
- [ ] Record confirmed stale Pod UIDs during request ingestion and make cache cleanup delete those UIDs even while they remain in the request snapshot.
- [ ] Split common allocation copying from Pod QoS derivation so synchronous metadata handling does not depend on a legacy Pod helper.
- [ ] Derive NUMA-binding and ordinary origin pool names from `ci.QoSLevel`, the final effective QoS value.
- [ ] Remove duplicate initialization that is immediately overwritten by the common apply path.
- [ ] Run focused and package tests, then commit only the SysAdvisor files.

### Task 2: Advisor stuck-target recovery safety

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`

- [ ] Add a phase table proving only an aged `physical_apply` target is recoverable.
- [ ] Update residual-cleanup and writer tests to place targets in `physical_apply`.
- [ ] Assert stuck recovery removes the target/checkpoints but retains `RetryReasonApplyFailed` and dirty canonical reconciliation.
- [ ] Run focused tests and verify the new assertions fail for the intended reasons.
- [ ] Restrict the shared stuck predicate to `physical_apply`.
- [ ] Split fence release from successful retry-bookkeeping retirement.
- [ ] On stuck recovery, release the fence and explicitly transfer ownership to the existing dirty canonical reconciliation loop.
- [ ] Correct comments that overstate stale-entry ownership and remove unrelated test fixtures.
- [ ] Run focused repetitions, the dynamicpolicy suite, race-sensitive tests, vet, formatting, and diff checks.
- [ ] Commit only the Advisor lifecycle files.

### Task 3: Final verification and review artifacts

**Files:**
- Create outside the repository: `stuck-target-systematic-review.html`
- Create outside the repository: `stuck-target-systematic-review.md`

- [ ] Run SysAdvisor and dynamicpolicy package tests.
- [ ] Run `go test -race` for the focused recovery tests.
- [ ] Run `go vet`, `gofmt -d`, and `git diff --check`.
- [ ] Review the final commit series for atomicity and ensure no code commit contains `docs/`.
- [ ] Generate the structured Markdown and HTML review reports with findings, fixes, test evidence, and commit hashes.
