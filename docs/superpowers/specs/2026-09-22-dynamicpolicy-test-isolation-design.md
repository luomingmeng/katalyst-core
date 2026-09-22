# Dynamic Policy Test Isolation Design

## Goal

Make the complete dynamic policy test tree deterministic on macOS/arm64 without changing production behavior or globally disabling useful test parallelism.

## Failure Boundaries

Three independent process-global state hazards currently prevent a reliable full test run:

1. `TestDynamicPolicy_checkAllPodsQuota` runs several sequential `PatchConvey` cases under one `testing.T`. A method patch from one case can remain observable by the next case.
2. CPU suppression subtests concurrently unregister and register the process-global generic reclaim consumer.
3. `TestAllocateSharedNumaBindingCPUsMarksColdStartRampUp` executes unpatched `PodFetcherStub` code while parallel Mockey tests rewrite nearby method text on arm64.

## Design

### Patch lifecycle isolation

Keep one `PatchConvey` scope for the quota test and patch each target exactly
once. Mutable per-scenario return values exercise each branch without repeatedly
rewriting the same method text. The test retains `t.Parallel()` and uses the
existing `advisorTestMutex` only around the Mockey critical section.

### Reclaim registry ownership

Add a package-local mutex for tests that replace `reclaim.GenericConsumerName`. The shared setup helper acquires the mutex before unregister/register and installs a `t.Cleanup` callback that unregisters the consumer and releases the mutex. The lock covers the complete test lifetime, not only the register call.

### ARM64 code-page safety

Retain `t.Parallel()` in
`TestAllocateSharedNumaBindingCPUsMarksColdStartRampUp`. Its initialization
helper already owns `advisorTestMutex`; after initialization returns, acquire the
same mutex around the unpatched `PodFetcherStub` call. This prevents the call
from racing adjacent Mockey text rewriting without serializing unrelated test
work or re-entering the mutex.

## Verification

Run the three previously failing tests independently first. Then run:

```bash
go test -p 1 -gcflags='all=-N -l' ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
```

Repeat the complete command to detect nondeterminism, then run focused race tests, `go vet`, `gofmt`, `git diff --check`, and a clean-worktree check after committing.

## Commit Boundaries

The design document is committed separately. All test-only changes form one atomic `test(qrm-cpu)` commit because they implement one test-isolation contract and do not modify production code.
