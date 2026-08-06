# Admission Missing-Pod Ramp-Up Implementation Plan

> **For agentic workers:** Execute this plan inline with a test-first red-green cycle.

**Goal:** Treat a temporary Pod-not-found result during admission as a shared-core ramp-up condition.

**Architecture:** Keep context propagation and the one-second lookup deadline unchanged. Change only the Pod-not-found branch in `shouldSharedCoresRampUp` to return `true`, then pin the contract with a focused unit test.

**Tech Stack:** Go, Go testing, testify.

---

### Task 1: Restore admission ramp-up for a missing Pod

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`

- [ ] **Step 1: Write the failing test**

```go
t.Run("default config treats missing pod as admission ramp up", func(t *testing.T) {
	policy, err := getTestDynamicPolicyWithInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	policy.metaServer.MetaAgent.PodFetcher = &pod.PodFetcherStub{PodList: nil}
	assert.True(t, policy.shouldSharedCoresRampUp(context.Background(), "missing-pod"))
})
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -run '^TestShouldSharedCoresRampUpDisabledByDynamicConfig/default_config_treats_missing_pod_as_admission_ramp_up$' -count=1`

Expected: FAIL because the current missing-Pod branch returns `false`.

- [ ] **Step 3: Write the minimal implementation**

```go
if isPodFetcherPodNotFoundError(err) {
	general.Infof("pod: %s is not yet visible, try to ramp up it", podUID)
	return true
}
```

- [ ] **Step 4: Run focused and package tests**

Run: `go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -run 'TestShouldSharedCoresRampUpDisabledByDynamicConfig|TestAllocate$' -count=1`

Expected: PASS, including the shared-NUMA reclaim allocation case.
