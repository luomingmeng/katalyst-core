# Dynamic Critical Watermark Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Memory Guard's critical watermark source dynamically configurable through AQC while preserving the existing startup flag as the fallback default.

**Architecture:** The AQC API exposes an optional `criticalWatermarkSource` enum under `MemoryGuardConfig`. Core moves the effective value into dynamic `MemoryGuardConfiguration`, seeds that configuration from the existing startup flag, and reads one dynamic snapshot per calculation. Adapter keeps its existing environment mapping and updates only API/Core module versions.

**Tech Stack:** Go, Kubernetes CRDs and kubebuilder markers, Katalyst dynamic configuration, Testify, Go modules, Bash parameter mapping.

---

## File Map

### API

- Modify `pkg/apis/config/v1alpha1/adminqos.go`: define the optional AQC field.
- Modify `pkg/apis/config/v1alpha1/adminqos_test.go`: cover JSON and deepcopy behavior.
- Modify `pkg/apis/config/v1alpha1/zz_generated.deepcopy.go`: generated pointer deepcopy.
- Modify `config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml`: generated enum schema.

### Core

- Modify `pkg/config/agent/dynamic/adminqos/advisor/memory_guard.go`: own the effective value and apply AQC overrides.
- Create `pkg/config/agent/dynamic/adminqos/advisor/memory_guard_test.go`: test defaults and projection.
- Modify `cmd/katalyst-agent/app/options/dynamic/adminqos/advisor/memory_guard.go`: preserve the flag while moving its target to dynamic defaults.
- Create `cmd/katalyst-agent/app/options/dynamic/adminqos/advisor/memory_guard_test.go`: test startup defaults and validation.
- Modify `cmd/katalyst-agent/app/options/sysadvisor/qosaware/resource/memory/memory_advisor.go`: remove static flag ownership.
- Modify `pkg/config/agent/sysadvisor/qosaware/resource/memory/memory_advisor.go`: remove the static field.
- Modify `pkg/agent/sysadvisor/plugin/qosaware/resource/memory/plugin/memory_guard.go`: consume one dynamic snapshot.
- Modify `pkg/agent/sysadvisor/plugin/qosaware/resource/memory/plugin/memory_guard_test.go`: exercise production watermark selection.
- Modify `pkg/metaserver/kcc/manager_test.go`: verify AQC removal restores startup defaults.
- Modify `go.mod` and `go.sum`: consume the API feature commit.

### Adapter

- Modify `go.mod` and `go.sum`: consume the API and Core feature commits.
- Verify `build/katalyst-agent/bytedance_run.sh`: existing mapping remains unchanged.

## Task 1: Add the AQC API Contract

**Worktree:** `katalyst-api-critical-watermark-source-aqc`

**Files:**
- Modify: `pkg/apis/config/v1alpha1/adminqos.go`
- Modify: `pkg/apis/config/v1alpha1/adminqos_test.go`
- Generate: `pkg/apis/config/v1alpha1/zz_generated.deepcopy.go`
- Generate: `config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml`

- [ ] **Step 1: Write failing API behavior tests**

Add a round-trip and deepcopy test using a pointer value:

```go
func TestMemoryGuardConfigCriticalWatermarkSourceRoundTrip(t *testing.T) {
	source := CriticalWatermarkSourceHigh
	in := &MemoryGuardConfig{CriticalWatermarkSource: &source}

	data, err := json.Marshal(in)
	require.NoError(t, err)
	require.JSONEq(t, `{"criticalWatermarkSource":"high"}`, string(data))

	out := &MemoryGuardConfig{}
	require.NoError(t, json.Unmarshal(data, out))
	require.NotNil(t, out.CriticalWatermarkSource)
	require.Equal(t, "high", *out.CriticalWatermarkSource)

	copied := in.DeepCopy()
	require.NotSame(t, in.CriticalWatermarkSource, copied.CriticalWatermarkSource)
	require.Equal(t, *in.CriticalWatermarkSource, *copied.CriticalWatermarkSource)
}
```

Extend the existing structured CRD schema test helper to assert:

```go
sourceSchema := findSchemaProperty(t, rootSchema,
	"spec", "config", "advisorConfig", "memoryAdvisorConfig",
	"memoryGuardConfig", "criticalWatermarkSource")
require.Equal(t, "string", sourceSchema["type"])
require.ElementsMatch(t, []interface{}{"low", "high"}, sourceSchema["enum"])
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
go test ./pkg/apis/config/v1alpha1
```

Expected: compilation fails because `MemoryGuardConfig.CriticalWatermarkSource` does not exist, or the schema assertion fails before regeneration.

- [ ] **Step 3: Add the API field**

Add to `MemoryGuardConfig`:

```go
type CriticalWatermarkSource string

const (
	CriticalWatermarkSourceLow  CriticalWatermarkSource = "low"
	CriticalWatermarkSourceHigh CriticalWatermarkSource = "high"
)

// +kubebuilder:validation:Enum=low;high
// +optional
CriticalWatermarkSource *CriticalWatermarkSource `json:"criticalWatermarkSource,omitempty"`
```

- [ ] **Step 4: Regenerate API artifacts**

Run:

```bash
make generate-manifests generate-go
```

Expected generated schema:

```yaml
criticalWatermarkSource:
  enum:
  - low
  - high
  type: string
```

Expected deepcopy behavior:

```go
if in.CriticalWatermarkSource != nil {
	in, out := &in.CriticalWatermarkSource, &out.CriticalWatermarkSource
	*out = new(string)
	**out = **in
}
```

- [ ] **Step 5: Run API tests and inspect generated scope**

Run:

```bash
go test ./pkg/apis/config/v1alpha1
git diff --check
git status --short
```

Expected: tests pass and only the four API files listed above change.

- [ ] **Step 6: Commit the API change**

```bash
git add pkg/apis/config/v1alpha1/adminqos.go \
  pkg/apis/config/v1alpha1/adminqos_test.go \
  pkg/apis/config/v1alpha1/zz_generated.deepcopy.go \
  config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml
git commit -m "feat(config): add dynamic critical watermark source" \
  -m "Expose criticalWatermarkSource under the AQC Memory Guard config with low and high enum validation." \
  -m "Keep the field optional so removing it restores the startup-provided dynamic default."
```

## Task 2: Move Configuration Ownership in Core

**Worktree:** `katalyst-core/.worktrees/critical-watermark-source-aqc`

**Prerequisite:** Push the API commit and resolve its pseudo-version without adding a local replace.

**Files:**
- Modify: `go.mod`
- Modify: `go.sum`
- Modify: `pkg/config/agent/dynamic/adminqos/advisor/memory_guard.go`
- Create: `pkg/config/agent/dynamic/adminqos/advisor/memory_guard_test.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/advisor/memory_guard.go`
- Create: `cmd/katalyst-agent/app/options/dynamic/adminqos/advisor/memory_guard_test.go`
- Modify: `cmd/katalyst-agent/app/options/sysadvisor/qosaware/resource/memory/memory_advisor.go`
- Modify: `pkg/config/agent/sysadvisor/qosaware/resource/memory/memory_advisor.go`

- [ ] **Step 1: Update Core to the API feature commit**

Resolve the pushed commit:

```bash
API_COMMIT=$(git -C /Users/bytedance/go/src/github.com/kubewharf/katalyst-api-critical-watermark-source-aqc rev-parse HEAD)
API_VERSION=$(go list -m -f '{{.Version}}' "github.com/luomingmeng/katalyst-api@${API_COMMIT}")
test -n "${API_VERSION}"
```

Update the existing remote replace:

```bash
go mod edit -replace="github.com/kubewharf/katalyst-api=github.com/luomingmeng/katalyst-api@${API_VERSION}"
go mod tidy
```

Expected: no local filesystem replace is present.

- [ ] **Step 2: Write failing dynamic configuration tests**

Create table-driven coverage for constructor and AQC projection:

```go
func TestMemoryGuardConfigurationApplyConfiguration(t *testing.T) {
	high := configapi.CriticalWatermarkSourceHigh
	conf := NewMemoryGuardConfiguration()
	require.Equal(t, configapi.CriticalWatermarkSourceLow, conf.CriticalWatermarkSource)

	conf.ApplyConfiguration(newMemoryGuardDynamicConfig(&high))
	require.Equal(t, configapi.CriticalWatermarkSourceHigh, conf.CriticalWatermarkSource)

	conf.ApplyConfiguration(newMemoryGuardDynamicConfig(nil))
	require.Equal(t, configapi.CriticalWatermarkSourceHigh, conf.CriticalWatermarkSource)
}
```

The helper must construct the complete `DynamicConfigCRD` path down to
`configapi.MemoryGuardConfig`.

- [ ] **Step 3: Write failing dynamic options tests**

Cover:

```go
func TestMemoryGuardOptionsApplyTo(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		want    configapi.CriticalWatermarkSource
		wantErr bool
	}{
		{name: "low", source: string(configapi.CriticalWatermarkSourceLow), want: configapi.CriticalWatermarkSourceLow},
		{name: "high", source: string(configapi.CriticalWatermarkSourceHigh), want: configapi.CriticalWatermarkSourceHigh},
		{name: "empty", source: "", want: configapi.CriticalWatermarkSourceLow},
		{name: "invalid", source: "critical", wantErr: true},
	}
	// Construct options and MemoryGuardConfiguration for each case.
}
```

Also assert `NewMemoryGuardOptions().CriticalWatermarkSource ==
string(configapi.CriticalWatermarkSourceLow)` and that `AddFlags` registers
`memory-advisor-critical-watermark-source`.

- [ ] **Step 4: Run focused tests and verify failure**

Run:

```bash
go test ./pkg/config/agent/dynamic/adminqos/advisor \
  ./cmd/katalyst-agent/app/options/dynamic/adminqos/advisor
```

Expected: compilation fails because the dynamic option and configuration fields do not exist.

- [ ] **Step 5: Implement the dynamic owner**

Add to `MemoryGuardConfiguration`:

```go
CriticalWatermarkSource configapi.CriticalWatermarkSource
```

Initialize and project it:

```go
CriticalWatermarkSource: configapi.CriticalWatermarkSourceLow,
```

```go
if config.CriticalWatermarkSource != nil {
	c.CriticalWatermarkSource = *config.CriticalWatermarkSource
}
```

- [ ] **Step 6: Move flag ownership to dynamic options**

Add `CriticalWatermarkSource string` to dynamic `MemoryGuardOptions`, default
it to `low`, register the unchanged flag name with `StringVar`, and convert to
the API enum type during `ApplyTo` validation:

```go
switch o.CriticalWatermarkSource {
case "":
	c.CriticalWatermarkSource = configapi.CriticalWatermarkSourceLow
case string(configapi.CriticalWatermarkSourceLow):
	c.CriticalWatermarkSource = configapi.CriticalWatermarkSourceLow
case string(configapi.CriticalWatermarkSourceHigh):
	c.CriticalWatermarkSource = configapi.CriticalWatermarkSourceHigh
default:
	return fmt.Errorf(
		"invalid --memory-advisor-critical-watermark-source %q, want \"low\" or \"high\"",
		o.CriticalWatermarkSource,
	)
}
```

Remove the field, default, flag registration, validation, and unused imports
from static Memory Advisor options. Remove
`MemoryAdvisorConfiguration.CriticalWatermarkSource`.

- [ ] **Step 7: Run focused configuration tests**

Run:

```bash
go test ./pkg/config/agent/dynamic/adminqos/advisor \
  ./cmd/katalyst-agent/app/options/dynamic/adminqos/advisor \
  ./cmd/katalyst-agent/app/options/sysadvisor/qosaware/resource/memory
```

Expected: all tests pass and static options compile without the former field.

- [ ] **Step 8: Commit ownership migration**

```bash
git add go.mod go.sum \
  pkg/config/agent/dynamic/adminqos/advisor/memory_guard.go \
  pkg/config/agent/dynamic/adminqos/advisor/memory_guard_test.go \
  cmd/katalyst-agent/app/options/dynamic/adminqos/advisor/memory_guard.go \
  cmd/katalyst-agent/app/options/dynamic/adminqos/advisor/memory_guard_test.go \
  cmd/katalyst-agent/app/options/sysadvisor/qosaware/resource/memory/memory_advisor.go \
  pkg/config/agent/sysadvisor/qosaware/resource/memory/memory_advisor.go
git commit -m "feat(config): make watermark source dynamic" \
  -m "Move CriticalWatermarkSource into dynamic Memory Guard configuration and seed it from the existing startup flag." \
  -m "Allow AQC to override the value while preserving startup validation and eliminating the former static runtime owner."
```

## Task 3: Consume Dynamic State and Verify Fallback

**Worktree:** `katalyst-core/.worktrees/critical-watermark-source-aqc`

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/memory/plugin/memory_guard.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/memory/plugin/memory_guard_test.go`
- Modify: `pkg/metaserver/kcc/manager_test.go`

- [ ] **Step 1: Replace the non-production watermark test**

Remove the local `pickWatermark` closure test. Add tests that call production
selection or the full calculation path with:

```go
tests := []struct {
	name       string
	source     string
	lowPages   uint64
	highPages  uint64
	wantPages  uint64
}{
	{name: "low", source: "low", lowPages: 10, highPages: 20, wantPages: 10},
	{name: "high", source: "high", lowPages: 10, highPages: 20, wantPages: 20},
}
```

Extract the selection into this focused production helper:

```go
func getCriticalWatermarkPages(zoneInfo *machine.ZoneInfo, source string) uint64
```

Call the helper from the production calculation and test the helper directly.
Do not duplicate the selection logic in the test. The KCC fallback test below
provides the dynamic update coverage.

- [ ] **Step 2: Add a failing KCC fallback test**

Use the manager test fixture to establish:

```text
default dynamic source: high
first AQC source: low
second AQC source: nil
effective source after second rebuild: high
```

The test must invoke the same `deepCopy(defaultConfig)` plus
`applyDynamicConfig` path used by the manager, not repeatedly mutate one
`MemoryGuardConfiguration`.

- [ ] **Step 3: Run focused tests and verify failure**

Run:

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/memory/plugin \
  ./pkg/metaserver/kcc
```

Expected: the production path still reads the removed static field or does not satisfy the new fallback assertion.

- [ ] **Step 4: Read one dynamic snapshot**

At the start of the relevant calculation, read:

```go
dynamicConfig := mg.conf.GetDynamicConfiguration()
```

Use that same snapshot for:

```go
watermarkPages := zoneInfo.Low
if dynamicConfig.CriticalWatermarkSource == "high" {
	watermarkPages = zoneInfo.High
}
criticalWatermark *= dynamicConfig.CriticalWatermarkScaleFactor
reclaimedMemoryMaxRatio := dynamicConfig.ReclaimedMemoryMaxRatio
```

Do not retain any static fallback.

- [ ] **Step 5: Run the Core verification set**

Run:

```bash
go test \
  ./cmd/katalyst-agent/app/options/dynamic/adminqos/advisor \
  ./cmd/katalyst-agent/app/options/sysadvisor/qosaware/resource/memory \
  ./pkg/config/agent/dynamic/adminqos/advisor \
  ./pkg/metaserver/kcc \
  ./pkg/agent/sysadvisor/plugin/qosaware/resource/memory/plugin
git diff --check
```

Expected: all packages pass.

- [ ] **Step 6: Commit runtime consumption**

```bash
git add pkg/agent/sysadvisor/plugin/qosaware/resource/memory/plugin/memory_guard.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/memory/plugin/memory_guard_test.go \
  pkg/metaserver/kcc/manager_test.go
git commit -m "feat(sysadvisor): consume dynamic watermark source" \
  -m "Select low or high zone watermarks from one dynamic configuration snapshot together with the scale factor and memory ratio." \
  -m "Verify AQC updates take effect without restart and field removal restores the startup default."
```

## Task 4: Update Adapter Dependencies

**Worktree:** `katalyst-adapter-critical-watermark-source-aqc`

**Prerequisite:** Push the final API and Core commits.

**Files:**
- Modify: `go.mod`
- Modify if changed: `go.sum`
- Verify unchanged: `build/katalyst-agent/bytedance_run.sh`

- [ ] **Step 1: Verify the existing flag mapping**

Run:

```bash
grep -F 'param_map["SysAdvisorMemoryAdvisorCriticalWatermarkSource"]="memory-advisor-critical-watermark-source"' \
  build/katalyst-agent/bytedance_run.sh
```

Expected: exactly one existing match. Do not edit the script.

- [ ] **Step 2: Resolve remote pseudo-versions**

Run:

```bash
API_COMMIT=$(git -C /Users/bytedance/go/src/github.com/kubewharf/katalyst-api-critical-watermark-source-aqc rev-parse HEAD)
CORE_COMMIT=$(git -C /Users/bytedance/go/src/github.com/kubewharf/katalyst-core/.worktrees/critical-watermark-source-aqc rev-parse HEAD)
API_VERSION=$(go list -m -f '{{.Version}}' "github.com/luomingmeng/katalyst-api@${API_COMMIT}")
CORE_VERSION=$(go list -m -f '{{.Version}}' "github.com/luomingmeng/katalyst-core@${CORE_COMMIT}")
test -n "${API_VERSION}" && test -n "${CORE_VERSION}"
```

- [ ] **Step 3: Update existing remote replaces**

Run:

```bash
go mod edit -replace="github.com/kubewharf/katalyst-api=github.com/luomingmeng/katalyst-api@${API_VERSION}"
go mod edit -replace="github.com/kubewharf/katalyst-core=github.com/luomingmeng/katalyst-core@${CORE_VERSION}"
go mod tidy
```

Verify:

```bash
git diff -- go.mod go.sum
```

Expected: only remote pseudo-versions and required checksums change; no local path replace appears.

- [ ] **Step 4: Run Adapter tests**

Run:

```bash
go test ./pkg/agent/sysadvisor/qosaware/memory/...
git diff --check
```

Expected: all packages pass.

- [ ] **Step 5: Commit Adapter dependency wiring**

```bash
git add go.mod go.sum
git commit -m "build(agent): update dynamic watermark dependencies" \
  -m "Consume the API and Core feature commits that expose CriticalWatermarkSource through AQC." \
  -m "Keep the existing adapter environment-to-flag mapping unchanged."
```

## Task 5: Cross-Repository Final Verification

**Worktrees:** all three feature worktrees

- [ ] **Step 1: Verify clean history boundaries**

Run:

```bash
git -C /Users/bytedance/go/src/github.com/kubewharf/katalyst-api-critical-watermark-source-aqc status --short
git -C /Users/bytedance/go/src/github.com/kubewharf/katalyst-api-critical-watermark-source-aqc log --oneline feat/default-share-residual-backfill-api..HEAD
git -C /Users/bytedance/go/src/github.com/kubewharf/katalyst-core/.worktrees/critical-watermark-source-aqc status --short
git -C /Users/bytedance/go/src/github.com/kubewharf/katalyst-core/.worktrees/critical-watermark-source-aqc log --oneline feat/default-share-residual-backfill..HEAD
git -C /Users/bytedance/go/src/github.com/kubewharf/katalyst-adapter-critical-watermark-source-aqc status --short
git -C /Users/bytedance/go/src/github.com/kubewharf/katalyst-adapter-critical-watermark-source-aqc log --oneline feat/default-share-residual-backfill-adapter..HEAD
```

Expected:

- API contains one implementation commit.
- Core contains the design commit and two implementation commits.
- Adapter contains one dependency commit.
- Every worktree is clean.

- [ ] **Step 2: Verify no duplicate owner remains**

Run in Core:

```bash
git grep -n 'CriticalWatermarkSource'
```

Expected references are limited to dynamic config/options, AQC projection,
runtime consumption, and tests. Static `MemoryAdvisorConfiguration` and static
Memory Advisor options contain no owner.

- [ ] **Step 3: Re-run repository-focused tests**

API:

```bash
go test ./pkg/apis/config/v1alpha1
```

Core:

```bash
go test \
  ./cmd/katalyst-agent/app/options/dynamic/adminqos/advisor \
  ./cmd/katalyst-agent/app/options/sysadvisor/qosaware/resource/memory \
  ./pkg/config/agent/dynamic/adminqos/advisor \
  ./pkg/metaserver/kcc \
  ./pkg/agent/sysadvisor/plugin/qosaware/resource/memory/plugin
```

Adapter:

```bash
go test ./pkg/agent/sysadvisor/qosaware/memory/...
```

Expected: all commands pass without modifying tracked files.
