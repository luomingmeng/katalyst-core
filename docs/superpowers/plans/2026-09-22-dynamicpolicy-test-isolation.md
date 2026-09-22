# Dynamic Policy Test Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the complete dynamic policy test tree deterministic by isolating Mockey patch lifetimes, reclaim registry ownership, and arm64 code-page execution.

**Architecture:** Keep production code unchanged. Give each quota test scenario its own `testing.T` and Mockey scope, serialize tests that replace the process-global generic reclaim consumer for their full lifetime, and prevent the cold-start allocation test from overlapping top-level Mockey patch users.

**Tech Stack:** Go `testing`, Mockey, Testify, Go test scheduler.

---

### Task 1: Isolate quota patch scopes

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`

- [ ] **Step 1: Reproduce the leaked patch**

Run:

```bash
go test -gcflags='all=-N -l' ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^TestDynamicPolicy_checkAllPodsQuota$' -count=1 -v
```

Expected: FAIL because the `getPodAndRelativePath` error patch from one `PatchConvey` case remains observable in the next case.

- [ ] **Step 2: Give every scenario an independent test lifetime**

Retain `t.Parallel()` and the existing `advisorTestMutex`. Replace the repeated
`PatchConvey` blocks with one patch scope whose callbacks read mutable
per-scenario errors:

```go
mockey.PatchConvey("checkAndApplyAllPodsQuota isolates scenario state", t, func() {
	var cgroupReadErr error
	mockey.Mock(cgroupmgr.GetCPUWithRelativePath).
		To(func(_ string) (*common.CPUStats, error) {
			return mockBG, cgroupReadErr
		}).Build()

	cgroupReadErr = mockErr
	assert.Error(t, p.checkAndApplyAllPodsQuota(mockCal, mockBG.CpuQuota))
})
```

Patch each target once. Mutate only the active scenario result, reset it before
the next scenario, and count downstream calls for the pod-path skip case.

- [ ] **Step 3: Verify the isolated test**

Run the Step 1 command again.

Expected: PASS.

### Task 2: Serialize reclaim registry replacement

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpueviction/strategy/pressure_suppression_test.go`

- [ ] **Step 1: Reproduce concurrent registry ownership**

Run:

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpueviction/strategy \
  -run '^TestCPUPressureSuppression_GetEvictPods$' -count=1
```

Expected: FAIL with `reclaim consumer "generic" is already registered`.

- [ ] **Step 2: Hold one lock for each test lifetime**

Add:

```go
var suppressionReclaimRegistryMu sync.Mutex
```

At the start of `makeSuppressionEvictionConf`, acquire the mutex. Register cleanup before mutating the registry so assertion failures cannot strand the lock:

```go
suppressionReclaimRegistryMu.Lock()
t.Cleanup(func() {
	reclaim.UnregisterConsumer(reclaim.GenericConsumerName)
	suppressionReclaimRegistryMu.Unlock()
})

reclaim.UnregisterConsumer(reclaim.GenericConsumerName)
require.NoError(t, reclaim.RegisterNamedGenericConsumer(reclaim.GenericConsumerName, conf, nil))
```

- [ ] **Step 3: Verify the registry test repeatedly**

Run:

```bash
go test -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpueviction/strategy \
  -run '^TestCPUPressureSuppression_GetEvictPods$' -count=10
```

Expected: PASS.

### Task 3: Prevent arm64 code-page overlap while retaining parallelism

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`

- [ ] **Step 1: Preserve the observed full-package crash evidence**

Use the existing full-suite log as RED evidence. It shows `SIGBUS` in the unpatched `PodFetcherStub.GetContainerSpec` call from `TestAllocateSharedNumaBindingCPUsMarksColdStartRampUp` while parallel Mockey tests are active.

- [ ] **Step 2: Make the vulnerable top-level test sequential**

Retain:

```go
t.Parallel()
```

Allow `getTestDynamicPolicyWithInitialization` to use its existing internal
`advisorTestMutex`, then acquire the same mutex after initialization and hold it
around the remaining unpatched `PodFetcherStub` path:

```go
policy, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
require.NoError(t, err)

advisorTestMutex.Lock()
defer advisorTestMutex.Unlock()
```

Do not acquire the mutex before initialization because that helper already owns
it and `sync.Mutex` is not reentrant.

- [ ] **Step 3: Run the complete test tree twice**

Run twice:

```bash
go test -p 1 -gcflags='all=-N -l' \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
```

Expected: PASS on both runs with no `SIGBUS`, registry collision, or assertion failure.

### Task 4: Final gates and atomic commit

**Files:**
- Verify all modified test files.

- [ ] **Step 1: Run race and static checks**

```bash
MOCKEY_CHECK_GCFLAGS=false go test -race \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run '^(TestDynamicPolicy_checkAllPodsQuota|TestAllocateSharedNumaBindingCPUsMarksColdStartRampUp)$' \
  -count=1

go vet ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
gofmt -w \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpueviction/strategy/pressure_suppression_test.go
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 2: Commit test-only changes**

```bash
git add \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpueviction/strategy/pressure_suppression_test.go
git commit -m "test(qrm-cpu): isolate dynamic policy global state"
```

- [ ] **Step 3: Verify the committed boundary**

```bash
git status --short
git show --stat --oneline HEAD
```

Expected: clean worktree and exactly three test files in the implementation commit.
