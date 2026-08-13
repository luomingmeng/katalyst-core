# Effective Pod Request Resources Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one correct Pod effective-request helper and make the legacy request-summing API use it without changing callers.

**Architecture:** `EffectivePodRequestResources` will independently aggregate app-container requests and Init Container request maxima, take the per-resource maximum, and add Pod overhead last. `SumUpPodRequestResources` will delegate to the new helper so the fixed behavior reaches all existing callers without signature changes.

**Tech Stack:** Go, Kubernetes `core/v1.ResourceList`, `resource.Quantity`, testify assertions.

---

### Task 1: Specify Effective Request Semantics

**Files:**
- Modify: `pkg/util/native/pods_test.go`

- [ ] **Step 1: Write the failing test**

Add `TestEffectivePodRequestResources` with a Pod that has:

```go
Containers: []v1.Container{
    {Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
        v1.ResourceCPU: resource.MustParse("375m"),
        v1.ResourceMemory: resource.MustParse("128Mi"),
    }}},
},
InitContainers: []v1.Container{
    {Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
        v1.ResourceCPU: resource.MustParse("2"),
        v1.ResourceMemory: resource.MustParse("64Mi"),
        v1.ResourceName("example.com/milli"): resource.MustParse("1500m"),
    }}},
},
Overhead: v1.ResourceList{
    v1.ResourceCPU: resource.MustParse("75m"),
    v1.ResourceMemory: resource.MustParse("32Mi"),
},
```

Assert CPU is `2075m`, memory is `160Mi`, and the extended resource is `1500m`. Also assert that `SumUpPodRequestResources(pod)` equals the new helper result.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/util/native -run TestEffectivePodRequestResources -count=1`

Expected: FAIL because `EffectivePodRequestResources` does not yet exist.

### Task 2: Implement the Canonical Helper

**Files:**
- Modify: `pkg/util/native/pods.go`
- Test: `pkg/util/native/pods_test.go`

- [ ] **Step 1: Implement minimal helper**

Add:

```go
func EffectivePodRequestResources(pod *v1.Pod) v1.ResourceList
```

Build an app request list with `AddResources`, retain the maximum Init Container quantity for each resource with `Quantity.Cmp`, then add overhead to the per-resource maximum. Return a fresh resource list.

- [ ] **Step 2: Delegate the legacy API**

Replace the body of:

```go
func SumUpPodRequestResources(pod *v1.Pod) v1.ResourceList
```

with:

```go
return EffectivePodRequestResources(pod)
```

- [ ] **Step 3: Run focused tests**

Run: `go test ./pkg/util/native -run 'TestEffectivePodRequestResources|TestGetPodRequestResources' -count=1`

Expected: PASS.

- [ ] **Step 4: Run native package tests**

Run: `go test ./pkg/util/native -count=1`

Expected: PASS.

### Task 3: Verify Downstream Compatibility

**Files:**
- Modify: `pkg/util/native/pods.go`
- Modify: `pkg/util/native/pods_test.go`

- [ ] **Step 1: Run impacted resource accounting tests**

Run: `go test ./pkg/agent/evictionmanager/plugin/resource ./pkg/agent/evictionmanager/plugin/network -count=1`

Expected: PASS.

- [ ] **Step 2: Check format and scope**

Run: `gofmt -w pkg/util/native/pods.go pkg/util/native/pods_test.go && git diff --check && git diff --stat`

Expected: no whitespace errors; only the helper implementation and tests are changed after the already-committed specification.

- [ ] **Step 3: Commit**

```bash
git add pkg/util/native/pods.go pkg/util/native/pods_test.go
git commit -m "fix(native): calculate effective pod requests"
```
