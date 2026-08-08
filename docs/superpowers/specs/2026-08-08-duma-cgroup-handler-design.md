# Duma Container Cgroup Path Handler Design

## Goal

Allow Katalyst agents built with `katalyst-adapter` to resolve container cgroup
paths for `io.containerd.duma.v2` workloads without adding Duma-specific runtime
knowledge to `katalyst-core`.

The change must preserve fail-closed behavior for malformed runtime metadata,
missing sandbox IDs, and nonexistent cgroup paths.

## Context

Pull request `kubewharf/katalyst-core#1229` changes container cgroup path handlers
from:

```go
func(...) (string, error)
```

to:

```go
func(...) (path string, skip bool, err error)
```

This lets runtime-specific handlers skip unrelated containers instead of
returning a terminal error.

Duma reports:

```text
runtimeType=io.containerd.duma.v2
containerID=<container-id>
sandboxID=<sandbox-id>
```

The OCI runtime spec may contain a container-ID cgroup path, but the live cgroup
filesystem contains only the sandbox-ID leaf:

```text
<pod-qos-path>/pod<uid>/<sandbox-id>
```

Unlike Kata, the Duma leaf has no `kata_` prefix.

## Repository Layout

### Core

Target branch:

```text
feat/rdt-rampup-bulkhead-merge
```

The existing worktree is reused. Pull request 1229 is cherry-picked as a single
commit. The local `go.mod` replace remains uncommitted.

### Adapter

Base branch:

```text
feat/rdt-rampup-bulkhead-merge-adapter
```

A new isolated worktree and feature branch are created. The existing adapter
worktree, including its local `go.mod` and run-script edits, is not modified.

## Architecture

### Core Runtime Handler Initializer

`katalyst-core/pkg/metaserver/agent/pod` owns the `RuntimePodFetcher` instance.
Adapter code cannot safely reconstruct or reach that instance.

Core therefore exposes a generic initializer registry:

```go
type RuntimeCgroupPathHandlerInitializer func(RuntimePodFetcher)

func RegisterRuntimeCgroupPathHandlerInitializer(
    initializer RuntimeCgroupPathHandlerInitializer,
)
```

`NewPodFetcher` invokes a stable snapshot of registered initializers after
creating the runtime fetcher and before returning the pod fetcher.

Requirements:

- The registry contains no runtime-specific policy.
- Nil initializers are rejected or ignored deterministically.
- Initializers receive the same `RuntimePodFetcher` used by the Kata handler.
- Registration and snapshot reads are race-safe.
- Initializer execution order is registration order.
- Kata registration remains backward compatible.

### Adapter Duma Handler

Adapter registers one initializer during package initialization. The
initializer constructs a Duma handler with the provided
`corepod.RuntimePodFetcher` and registers absolute and relative cgroup path
handlers.

The Duma handler:

1. Calls `RuntimePodFetcher.GetContainerInfo(containerID)`.
2. Decodes the CRI `info` payload.
3. Returns `skip=true` when `runtimeType` does not contain
   `io.containerd.duma`.
4. Returns an error when runtime type is empty, metadata is malformed, or
   `sandboxID` is empty.
5. Builds:

   ```text
   pod<uid>/<sandboxID>
   ```

6. Resolves the candidate through the existing Kubernetes cgroup layout
   helpers.
7. Returns an error when the candidate path does not exist.

The default and Kata handlers remain responsible for their own runtime layouts.

## Registration

The adapter Duma package must be imported by the adapter agent startup path so
its initializer is registered before `metaserver.NewMetaServer` creates the pod
fetcher.

Registration uses `sync.Once` to prevent duplicate absolute or relative
handlers in tests and repeated initialization paths.

## Error Semantics

| Condition | Result |
| --- | --- |
| Non-Duma runtime | `skip=true`, no error |
| Duma runtime with valid sandbox path | resolved path |
| Empty runtime type | error |
| Invalid CRI info JSON | error |
| Missing sandbox ID | error |
| Pod cgroup missing | error |
| Sandbox cgroup missing | error |
| Runtime fetcher unavailable | error |

All new error messages start with lowercase letters.

## Testing

### Core

- Cherry-picked PR 1229 tests pass unchanged.
- Runtime initializer registry preserves registration order.
- Concurrent registration and snapshot execution are race-free.
- Nil runtime fetcher is passed through without panic; handlers decide whether
  it is usable.
- Existing Kata and default handler tests remain green.

### Adapter

- Duma absolute cgroup path resolves to the sandbox-ID leaf.
- Duma relative cgroup path resolves to the sandbox-ID leaf.
- Non-Duma runtime skips.
- Missing runtime type fails.
- Invalid JSON fails.
- Missing sandbox ID fails.
- Missing Pod or sandbox cgroup fails.
- Duplicate registration does not duplicate handlers.

### Integration

- Core and adapter focused tests pass with `-race`.
- A linux/amd64 agent builds from the paired worktrees.
- QRM and Sysadvisor run the same binary SHA.
- The existing live `io.containerd.duma.v2` Pod no longer causes
  `GetContainerRelativeCgroupPath` failure.
- Bulkhead target mode reaches `health_ready=true` and
  `EXPECTED_STATE=OK mode=target`.
- Standard and high-churn E2E run only after target convergence.
- Final reset must pass before the node is considered restored.

## Commit Structure

1. Cherry-pick PR 1229 into core without modification.
2. Add the generic runtime handler initializer registry and tests in core.
3. Add Duma handler and tests in adapter.
4. Wire the adapter package into agent startup.
5. Add or update integration/build coverage only if required.

Each commit remains independently reviewable. Local module replaces are never
committed.
