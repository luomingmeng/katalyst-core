# Duma Cgroup Path Handler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cherry-pick core PR 1229 and add an adapter-owned handler that resolves `io.containerd.duma.v2` containers to their sandbox-ID cgroup leaf.

**Architecture:** Core owns the CRI `RuntimePodFetcher`, so it exposes a runtime-handler initializer registry with no Duma policy. Adapter registers a Duma initializer at startup, reads CRI runtime metadata, skips non-Duma workloads, and resolves `pod<uid>/<sandboxID>` through existing cgroup helpers.

**Tech Stack:** Go, CRI runtime metadata, cgroup v1 path helpers, Git worktrees, Go unit/race tests, linux/amd64 agent build, real-node QRM E2E.

---

## File Map

### Core worktree

- Modify through cherry-pick:
  - `pkg/util/cgroup/common/types.go`
  - `pkg/util/cgroup/common/path.go`
  - `pkg/util/cgroup/common/path_test.go`
  - `pkg/metaserver/agent/pod/kata.go`
  - `pkg/metaserver/agent/pod/kata_test.go`
  - `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_irq_tuner_test.go`
- Create: `pkg/metaserver/agent/pod/runtime_cgroup_handler.go`
- Create: `pkg/metaserver/agent/pod/runtime_cgroup_handler_test.go`
- Modify: `pkg/metaserver/agent/pod/pod.go`

### Adapter worktree

- Create: `pkg/metaserver/agent/duma/duma_cgroup.go`
- Create: `pkg/metaserver/agent/duma/duma_cgroup_test.go`
- Modify: `cmd/katalyst-agent/app/agent.go`
- Local-only: `go.mod` replace entries for the paired core and API worktrees

---

### Task 1: Cherry-pick Core PR 1229

**Files:**
- Modify: the six files listed under “Modify through cherry-pick”

- [ ] **Step 1: Fetch the pull request commit**

```bash
git fetch origin pull/1229/head
git show --stat --oneline b45f2352de545dafc4f00faaa9e9193754142a12
```

Expected: one commit named `feat(cgroup): support skipping path handlers`.

- [ ] **Step 2: Cherry-pick without staging local `go.mod`**

```bash
git cherry-pick b45f2352de545dafc4f00faaa9e9193754142a12
```

Expected: clean cherry-pick; `go.mod` remains the only pre-existing local modification.

- [ ] **Step 3: Run the PR tests**

```bash
go test ./pkg/util/cgroup/common ./pkg/metaserver/agent/pod -count=1
go test -race ./pkg/util/cgroup/common ./pkg/metaserver/agent/pod -count=1
```

Expected: both commands pass.

- [ ] **Step 4: Audit handler callers**

```bash
git grep 'AbsoluteCgroupPathHandler{' -- '*.go'
git grep 'RelativeCgroupPathHandler{' -- '*.go'
```

Expected: every handler now returns `(string, bool, error)`.

---

### Task 2: Add the Core Runtime Initializer Registry

**Files:**
- Create: `pkg/metaserver/agent/pod/runtime_cgroup_handler.go`
- Create: `pkg/metaserver/agent/pod/runtime_cgroup_handler_test.go`
- Modify: `pkg/metaserver/agent/pod/pod.go`

- [ ] **Step 1: Write failing registry tests**

Create `runtime_cgroup_handler_test.go` with same-package tests:

```go
func TestInitializeRuntimeCgroupPathHandlersPreservesOrder(t *testing.T) {
    old := runtimeCgroupPathHandlerInitializers
    runtimeCgroupPathHandlerInitializers = runtimeCgroupPathHandlerInitializerRegistry{}
    t.Cleanup(func() { runtimeCgroupPathHandlerInitializers = old })

    got := make([]int, 0, 2)
    RegisterRuntimeCgroupPathHandlerInitializer(func(RuntimePodFetcher) {
        got = append(got, 1)
    })
    RegisterRuntimeCgroupPathHandlerInitializer(func(RuntimePodFetcher) {
        got = append(got, 2)
    })

    initializeRuntimeCgroupPathHandlers(nil)
    require.Equal(t, []int{1, 2}, got)
}

func TestRegisterRuntimeCgroupPathHandlerInitializerIgnoresNil(t *testing.T) {
    old := runtimeCgroupPathHandlerInitializers
    runtimeCgroupPathHandlerInitializers = runtimeCgroupPathHandlerInitializerRegistry{}
    t.Cleanup(func() { runtimeCgroupPathHandlerInitializers = old })

    RegisterRuntimeCgroupPathHandlerInitializer(nil)
    require.Empty(t, runtimeCgroupPathHandlerInitializers.snapshot())
}
```

- [ ] **Step 2: Verify the tests fail**

```bash
go test ./pkg/metaserver/agent/pod \
  -run 'Test(InitializeRuntimeCgroupPathHandlers|RegisterRuntimeCgroupPathHandlerInitializer)' \
  -count=1
```

Expected: compile failure because the registry symbols do not exist.

- [ ] **Step 3: Implement the registry**

Create `runtime_cgroup_handler.go`:

```go
package pod

import "sync"

type RuntimeCgroupPathHandlerInitializer func(RuntimePodFetcher)

type runtimeCgroupPathHandlerInitializerRegistry struct {
    sync.RWMutex
    initializers []RuntimeCgroupPathHandlerInitializer
}

var runtimeCgroupPathHandlerInitializers runtimeCgroupPathHandlerInitializerRegistry

func RegisterRuntimeCgroupPathHandlerInitializer(initializer RuntimeCgroupPathHandlerInitializer) {
    if initializer == nil {
        return
    }
    runtimeCgroupPathHandlerInitializers.Lock()
    defer runtimeCgroupPathHandlerInitializers.Unlock()
    runtimeCgroupPathHandlerInitializers.initializers =
        append(runtimeCgroupPathHandlerInitializers.initializers, initializer)
}

func (r *runtimeCgroupPathHandlerInitializerRegistry) snapshot() []RuntimeCgroupPathHandlerInitializer {
    r.RLock()
    defer r.RUnlock()
    return append([]RuntimeCgroupPathHandlerInitializer(nil), r.initializers...)
}

func initializeRuntimeCgroupPathHandlers(runtimePodFetcher RuntimePodFetcher) {
    for _, initializer := range runtimeCgroupPathHandlerInitializers.snapshot() {
        initializer(runtimePodFetcher)
    }
}
```

- [ ] **Step 4: Invoke initializers from `NewPodFetcher`**

Immediately after Kata registration in `pod.go`:

```go
RegisterKataContainerFetcher(runtimePodFetcher)
initializeRuntimeCgroupPathHandlers(runtimePodFetcher)
```

The default and Kata handlers remain registered before adapter handlers.

- [ ] **Step 5: Run focused and race tests**

```bash
go test ./pkg/metaserver/agent/pod -count=1
go test -race ./pkg/metaserver/agent/pod -count=1
```

Expected: pass with no race.

- [ ] **Step 6: Commit**

```bash
git add pkg/metaserver/agent/pod/runtime_cgroup_handler.go \
  pkg/metaserver/agent/pod/runtime_cgroup_handler_test.go \
  pkg/metaserver/agent/pod/pod.go
git commit -m "feat(metaserver): register runtime cgroup handlers" \
  -m "Expose a race-safe initializer registry that gives adapter-owned runtime handlers the same CRI fetcher used by the core Kata handler."
```

---

### Task 3: Add the Adapter Duma Handler

**Files:**
- Create: `pkg/metaserver/agent/duma/duma_cgroup.go`
- Create: `pkg/metaserver/agent/duma/duma_cgroup_test.go`
- Local-only: `go.mod`

- [ ] **Step 1: Link the adapter worktree to paired dependencies**

```bash
go mod edit \
  -replace=github.com/kubewharf/katalyst-core=/Users/bytedance/go/src/github.com/kubewharf/katalyst-core/.worktrees/rdt-rampup-bulkhead-merge
go mod edit \
  -replace=github.com/kubewharf/katalyst-api=/Users/bytedance/go/src/github.com/kubewharf/katalyst-api-rdt-rampup-merge
```

Expected: only local replace lines change; they must never be staged.

- [ ] **Step 2: Write failing pure Duma metadata tests**

Create `duma_cgroup_test.go`:

```go
type runtimePodFetcherStub struct {
    info map[string]map[string]string
}

func (s *runtimePodFetcherStub) GetPods(bool) ([]*corepod.RuntimePod, error) {
    return nil, nil
}

func (s *runtimePodFetcherStub) GetContainerInfo(containerID string) (map[string]string, error) {
    info, ok := s.info[containerID]
    if !ok {
        return nil, fmt.Errorf("container %s not found", containerID)
    }
    return info, nil
}

func TestDumaCgroupPathSuffix(t *testing.T) {
    fetcher := &runtimePodFetcherStub{info: map[string]map[string]string{
        "container": {
            "info": `{"runtimeType":"io.containerd.duma.v2","sandboxID":"sandbox"}`,
        },
    }}
    handler := &dumaCgroupPathHandler{runtimePodFetcher: fetcher}

    suffix, skip, err := handler.getDumaCgroupPathSuffix("pod", "container")
    require.NoError(t, err)
    require.False(t, skip)
    require.Equal(t, "podpod/sandbox", suffix)
}

func TestDumaCgroupPathSuffixSkipsNonDumaRuntime(t *testing.T) {
    fetcher := &runtimePodFetcherStub{info: map[string]map[string]string{
        "container": {
            "info": `{"runtimeType":"io.containerd.runc.v2","sandboxID":"sandbox"}`,
        },
    }}
    handler := &dumaCgroupPathHandler{runtimePodFetcher: fetcher}

    suffix, skip, err := handler.getDumaCgroupPathSuffix("pod", "container")
    require.NoError(t, err)
    require.True(t, skip)
    require.Empty(t, suffix)
}
```

Add the remaining cases explicitly:

```go
tests := []struct {
    name     string
    info     string
    fetchErr error
    wantSkip bool
    wantErr  string
}{
    {name: "malformed json", info: `{`, wantErr: "failed to unmarshal container info"},
    {name: "empty runtime", info: `{"sandboxID":"sandbox"}`, wantErr: "runtime type is empty"},
    {
        name: "empty sandbox",
        info: `{"runtimeType":"io.containerd.duma.v2"}`,
        wantErr: "sandbox id is empty",
    },
    {name: "runtime fetch error", fetchErr: errors.New("cri unavailable"), wantErr: "failed to get container info"},
}
```

- [ ] **Step 3: Verify the tests fail**

```bash
go test ./pkg/metaserver/agent/duma -count=1
```

Expected: compile failure because the package implementation does not exist.

- [ ] **Step 4: Implement Duma metadata and path resolution**

Create `duma_cgroup.go`:

```go
package duma

import (
    "fmt"
    "path"
    "strings"
    "sync"

    "k8s.io/apimachinery/pkg/util/json"

    corepod "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
    "github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
)

const (
    dumaCgroupPathHandlerName = "duma"
    dumaRuntimeType = "io.containerd.duma"
)

var registerDumaCgroupPathHandlerOnce sync.Once

type dumaCgroupPathHandler struct {
    runtimePodFetcher corepod.RuntimePodFetcher
    resolveAbsolute func(subsys, suffix string) (string, error)
    resolveRelative func(suffix string) (string, error)
}

func RegisterDumaCgroupPathHandler(runtimePodFetcher corepod.RuntimePodFetcher) {
    handler := &dumaCgroupPathHandler{
        runtimePodFetcher: runtimePodFetcher,
        resolveAbsolute: common.GetKubernetesAnyExistAbsCgroupPath,
        resolveRelative: common.GetKubernetesAnyExistRelativeCgroupPath,
    }
    registerDumaCgroupPathHandlerOnce.Do(func() {
        common.RegisterAbsoluteCgroupPathHandler(common.AbsoluteCgroupPathHandler{
            Name: dumaCgroupPathHandlerName,
            Handler: handler.getDumaContainerAbsoluteCgroupPath,
        })
        common.RegisterRelativeCgroupPathHandler(common.RelativeCgroupPathHandler{
            Name: dumaCgroupPathHandlerName,
            Handler: handler.getDumaContainerRelativeCgroupPath,
        })
    })
}

func (h *dumaCgroupPathHandler) getDumaCgroupPathSuffix(
    podUID, containerID string,
) (string, bool, error) {
    if h.runtimePodFetcher == nil {
        return "", false, fmt.Errorf("runtime pod fetcher is nil")
    }
    infoRaw, err := h.runtimePodFetcher.GetContainerInfo(containerID)
    if err != nil {
        return "", false, fmt.Errorf("failed to get container info: %v", err)
    }
    var info corepod.ContainerInfo
    if err := json.Unmarshal([]byte(infoRaw["info"]), &info); err != nil {
        return "", false, fmt.Errorf("failed to unmarshal container info: %v", err)
    }
    if info.RuntimeType == "" {
        return "", false, fmt.Errorf("runtime type is empty")
    }
    if !strings.Contains(info.RuntimeType, dumaRuntimeType) {
        return "", true, nil
    }
    if info.SandboxID == "" {
        return "", false, fmt.Errorf("sandbox id is empty")
    }
    return path.Join(common.PodCgroupPathPrefix+podUID, info.SandboxID), false, nil
}
```

Add the complete resolver methods:

```go
func (h *dumaCgroupPathHandler) getDumaContainerAbsoluteCgroupPath(
    subsys, podUID, containerID string,
) (string, bool, error) {
    suffix, skip, err := h.getDumaCgroupPathSuffix(podUID, containerID)
    if skip || err != nil {
        return "", skip, err
    }
    resolved, err := h.resolveAbsolute(subsys, suffix)
    if err != nil {
        return "", false, fmt.Errorf("failed to resolve Duma absolute cgroup path: %v", err)
    }
    return resolved, false, nil
}

func (h *dumaCgroupPathHandler) getDumaContainerRelativeCgroupPath(
    podUID, containerID string,
) (string, bool, error) {
    suffix, skip, err := h.getDumaCgroupPathSuffix(podUID, containerID)
    if skip || err != nil {
        return "", skip, err
    }
    resolved, err := h.resolveRelative(suffix)
    if err != nil {
        return "", false, fmt.Errorf("failed to resolve Duma relative cgroup path: %v", err)
    }
    return resolved, false, nil
}
```

- [ ] **Step 5: Add resolver tests**

Use resolver stubs so the unit test does not depend on the host cgroup layout:

```go
func TestDumaContainerRelativeCgroupPath(t *testing.T) {
    fetcher := &runtimePodFetcherStub{info: map[string]map[string]string{
        "container": {
            "info": `{"runtimeType":"io.containerd.duma.v2","sandboxID":"sandbox"}`,
        },
    }}
    handler := &dumaCgroupPathHandler{
        runtimePodFetcher: fetcher,
        resolveRelative: func(suffix string) (string, error) {
            require.Equal(t, "podpod/sandbox", suffix)
            return "kubepods/burstable/podpod/sandbox", nil
        },
    }

    got, skip, err := handler.getDumaContainerRelativeCgroupPath("pod", "container")
    require.NoError(t, err)
    require.False(t, skip)
    require.Equal(t, "kubepods/burstable/podpod/sandbox", got)
}

func TestDumaContainerRelativeCgroupPathFailsWhenSandboxLeafIsMissing(t *testing.T) {
    fetcher := &runtimePodFetcherStub{info: map[string]map[string]string{
        "container": {
            "info": `{"runtimeType":"io.containerd.duma.v2","sandboxID":"sandbox"}`,
        },
    }}
    handler := &dumaCgroupPathHandler{
        runtimePodFetcher: fetcher,
        resolveRelative: func(string) (string, error) {
            return "", os.ErrNotExist
        },
    }

    _, skip, err := handler.getDumaContainerRelativeCgroupPath("pod", "container")
    require.False(t, skip)
    require.ErrorContains(t, err, "failed to resolve Duma relative cgroup path")
}
```

Mirror the success case for the absolute resolver. Add a non-Duma case whose
resolver stub fails the test if called, proving skip happens before filesystem
lookup.

- [ ] **Step 6: Run focused and race tests**

```bash
go test ./pkg/metaserver/agent/duma -count=1
go test -race ./pkg/metaserver/agent/duma -count=1
```

Expected: pass.

- [ ] **Step 7: Commit without `go.mod`**

```bash
git add pkg/metaserver/agent/duma/duma_cgroup.go \
  pkg/metaserver/agent/duma/duma_cgroup_test.go
git commit -m "feat(metaserver): resolve Duma sandbox cgroups" \
  -m "Register adapter-owned Duma handlers that derive sandbox-ID cgroup leaves from CRI metadata and skip unrelated runtimes."
```

---

### Task 4: Wire Duma Registration into Agent Startup

**Files:**
- Modify: `cmd/katalyst-agent/app/agent.go`
- Test: `pkg/metaserver/agent/duma/duma_cgroup_test.go`

- [ ] **Step 1: Add a registration-observation test**

Expose only a same-package test helper that invokes the exported registration
function twice with a stub fetcher and confirms `sync.Once` prevents duplicate
registration side effects.

- [ ] **Step 2: Add the startup import**

Add to `cmd/katalyst-agent/app/agent.go` imports:

```go
_ "github.com/kubewharf/katalyst-adapter/pkg/metaserver/agent/duma"
```

The import must occur in the process that constructs `metaserver.NewMetaServer`.

- [ ] **Step 3: Compile the Duma package and agent**

```bash
go test ./pkg/metaserver/agent/duma -count=1
GO111MODULE=on GOOS=linux GOARCH=amd64 GOFLAGS='-tags=SKIPCGO' \
  go build -o /tmp/katalyst-agent-duma ./cmd/katalyst-agent/main.go
```

Expected: tests and linux build pass.

- [ ] **Step 4: Commit**

```bash
git add cmd/katalyst-agent/app/agent.go
git commit -m "feat(agent): register Duma cgroup resolver" \
  -m "Load the adapter Duma package before MetaServer initialization so it receives the core CRI runtime fetcher."
```

---

### Task 5: Cross-Repository Verification

**Files:**
- No production file changes expected

- [ ] **Step 1: Run core verification**

```bash
go test ./pkg/util/cgroup/common ./pkg/metaserver/agent/pod -count=1
go test -race ./pkg/util/cgroup/common ./pkg/metaserver/agent/pod -count=1
git diff --check
```

Expected: pass; core `go.mod` remains the only local modification.

- [ ] **Step 2: Run adapter verification**

```bash
go test ./pkg/metaserver/agent/duma -count=1
go test -race ./pkg/metaserver/agent/duma -count=1
GO111MODULE=on GOOS=linux GOARCH=amd64 GOFLAGS='-tags=SKIPCGO' \
  go build -o /tmp/katalyst-agent-duma ./cmd/katalyst-agent/main.go
git diff --check
```

Expected: pass; adapter `go.mod` replace remains uncommitted.

- [ ] **Step 3: Review both commit ranges**

Review core from `a3cb1d78b` to HEAD and adapter from `04e27695` to HEAD. Block
delivery on any P1 finding.

---

### Task 6: Real-Node Regression

**Files:**
- Build artifact only; no source changes expected

- [ ] **Step 1: Record the binary SHA**

```bash
shasum -a 256 /tmp/katalyst-agent-duma
```

- [ ] **Step 2: Upload and deploy to QRM and Sysadvisor**

Use the architecture jump-host two-stage transfer. Back up both binaries and
run scripts. Verify `/proc/<pid>/exe` and runsv-root identities match the new
SHA before enabling target mode.

- [ ] **Step 3: Verify the existing live Duma Pod**

Use the Pod/container identifiers recorded in:

```text
qrm-bulkhead-test-artifacts/dual_bf_fsx_cgroup_suffix_root_cause.md
```

Expected:

- Duma relative path resolves to `pod<uid>/<sandboxID>`;
- no `runtime type of container is not kata runtime` terminal error;
- no `failed to get relative cgroup path` for the live Duma Pod.

- [ ] **Step 4: Run the full E2E**

Run reset, target, standard 3 rounds, high-churn 5 rounds, and final reset with
a new RUN_TAG.

Expected:

```text
PHASE_DONE reset_dryrun rc=0
PHASE_DONE reset_actual rc=0
PHASE_DONE target_dryrun rc=0
PHASE_DONE target_actual rc=0
PHASE_DONE standard_3rounds rc=0
PHASE_DONE high_churn_5rounds rc=0
PHASE_DONE final_reset rc=0
FULL_E2E_DONE ... rc=0 final_reset_rc=0
```

- [ ] **Step 5: Package and retrieve logs**

Use `package_e2e_logs.sh`, verify remote/local SHA256 and `tar -tzf`, save under
`qrm-bulkhead-test-artifacts/`, and clean jump-host temporary files.
