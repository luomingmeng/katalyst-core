# Default Share CAT Ways Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose bulkhead default and per-CLOS CAT way counts through core flags and adapter environment variables.

**Architecture:** `CPUPluginOptions` owns startup values and converts Kubernetes StringToString input into the typed dynamic QRM configuration. katalyst-adapter only maps environment variable names to core flag names; existing dynamic AdminQoSConfiguration application remains the later override.

**Tech Stack:** Go, pflag through Kubernetes `NamedFlagSets`, Bash, Go tests.

---

## File Map

- Modify `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go` to register, validate, convert, and apply CAT way startup options.
- Modify `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go` to cover flag presence, parsing, conversion, defaults, and validation.
- Modify adapter `build/katalyst-agent/bytedance_run.sh` to map the two deployment environment variables.

### Task 1: Core flag contracts

**Files:**
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go`

- [ ] **Step 1: Write failing registration and valid parsing tests**

Extend the flag-registration table with:

```go
"bulkhead-default-cat-ways",
"bulkhead-clos-cat-ways",
```

Add a test that parses:

```go
[]string{
	"--bulkhead-default-cat-ways=4",
	"--bulkhead-clos-cat-ways=reclaim=2,shared=3",
}
```

Apply the options and assert:

```go
rdt := config.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig
if rdt.DefaultCATWays != 4 {
	t.Fatalf("DefaultCATWays = %d, want 4", rdt.DefaultCATWays)
}
if !reflect.DeepEqual(rdt.ClosCATWays, map[string]int64{"reclaim": 2, "shared": 3}) {
	t.Fatalf("ClosCATWays = %v, want reclaim=2,shared=3", rdt.ClosCATWays)
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm -run 'TestQRMPluginOptions_(AddFlags|ParseBulkheadCATWays)' -count=1
```

Expected: FAIL because both CAT way flags are absent.

- [ ] **Step 3: Add the minimal option fields and flags**

Add these fields:

```go
BulkheadDefaultCATWays int64
BulkheadClosCATWays    map[string]string
```

Register:

```go
fs.Int64Var(&o.BulkheadDefaultCATWays, "bulkhead-default-cat-ways", o.BulkheadDefaultCATWays,
	"default CAT way count for non-root bulkhead CLOS groups.")
fs.StringToStringVar(&o.BulkheadClosCATWays, "bulkhead-clos-cat-ways", o.BulkheadClosCATWays,
	"per-CLOS CAT way counts in clos=ways format.")
```

- [ ] **Step 4: Add minimal conversion in `ApplyTo`**

Convert each string with `strconv.ParseInt(value, 10, 64)` and write:

```go
c.BulkheadConfig.BulkheadRDTConfig.DefaultCATWays = o.BulkheadDefaultCATWays
c.BulkheadConfig.BulkheadRDTConfig.ClosCATWays = closCATWays
```

Leave `ClosCATWays` nil when the flag is omitted.

- [ ] **Step 5: Run the test and verify GREEN**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm -run 'TestQRMPluginOptions_(AddFlags|ParseBulkheadCATWays)' -count=1
```

Expected: PASS.

### Task 2: Core validation

**Files:**
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go`

- [ ] **Step 1: Write failing table-driven validation tests**

Cover these option values and expected lower-case error fragments:

```go
[]struct {
	name            string
	defaultCATWays  int64
	closCATWays     map[string]string
	wantErrContains string
}{
	{name: "negative default", defaultCATWays: -1, wantErrContains: "bulkhead-default-cat-ways must be positive"},
	{name: "empty clos", closCATWays: map[string]string{"": "2"}, wantErrContains: "bulkhead-clos-cat-ways contains an empty clos"},
	{name: "non integer", closCATWays: map[string]string{"reclaim": "x"}, wantErrContains: "invalid bulkhead-clos-cat-ways value"},
	{name: "zero ways", closCATWays: map[string]string{"reclaim": "0"}, wantErrContains: "bulkhead-clos-cat-ways value must be positive"},
	{name: "negative ways", closCATWays: map[string]string{"reclaim": "-1"}, wantErrContains: "bulkhead-clos-cat-ways value must be positive"},
}
```

Also assert that zero `BulkheadDefaultCATWays` and nil `BulkheadClosCATWays`
apply successfully and preserve zero/nil configuration.

- [ ] **Step 2: Run the validation test and verify RED**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm -run TestQRMPluginOptions_ValidateBulkheadCATWays -count=1
```

Expected: FAIL because invalid values are accepted or produce incomplete errors.

- [ ] **Step 3: Implement minimal validation**

Use lower-case errors and these rules:

```go
if o.BulkheadDefaultCATWays < 0 {
	return fmt.Errorf("bulkhead-default-cat-ways must be positive when configured, got %d", o.BulkheadDefaultCATWays)
}
```

For every CLOS entry:

```go
if clos == "" {
	return fmt.Errorf("bulkhead-clos-cat-ways contains an empty clos")
}
ways, err := strconv.ParseInt(rawWays, 10, 64)
if err != nil {
	return fmt.Errorf("invalid bulkhead-clos-cat-ways value %q for clos %q: %w", rawWays, clos, err)
}
if ways <= 0 {
	return fmt.Errorf("bulkhead-clos-cat-ways value must be positive for clos %q, got %d", clos, ways)
}
```

Zero default means “not configured” so existing startup behavior remains unchanged.

- [ ] **Step 4: Run all package tests and verify GREEN**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm -count=1
```

Expected: PASS.

- [ ] **Step 5: Format and commit core implementation**

Run:

```bash
gofmt -w cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go \
  cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go
git diff --check
git add cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go \
  cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go
git commit -m "feat(qrm): configure bulkhead CAT ways from flags"
```

### Task 3: Adapter environment mappings

**Files:**
- Modify: adapter `build/katalyst-agent/bytedance_run.sh`

- [ ] **Step 1: Verify the mapping contract is initially absent**

Run:

```bash
! grep -F 'param_map["QRMCPUPluginBulkheadDefaultCATWays"]="bulkhead-default-cat-ways"' build/katalyst-agent/bytedance_run.sh
! grep -F 'param_map["QRMCPUPluginBulkheadClosCATWays"]="bulkhead-clos-cat-ways"' build/katalyst-agent/bytedance_run.sh
```

Expected: both negative checks succeed because the mappings do not exist.

- [ ] **Step 2: Add the minimal mappings**

Add next to the existing bulkhead CPU plugin parameters:

```bash
param_map["QRMCPUPluginBulkheadDefaultCATWays"]="bulkhead-default-cat-ways"
param_map["QRMCPUPluginBulkheadClosCATWays"]="bulkhead-clos-cat-ways"
```

- [ ] **Step 3: Verify mapping and shell syntax**

Run:

```bash
grep -F 'param_map["QRMCPUPluginBulkheadDefaultCATWays"]="bulkhead-default-cat-ways"' build/katalyst-agent/bytedance_run.sh
grep -F 'param_map["QRMCPUPluginBulkheadClosCATWays"]="bulkhead-clos-cat-ways"' build/katalyst-agent/bytedance_run.sh
bash -n build/katalyst-agent/bytedance_run.sh
git diff --check
```

Expected: both mappings print once and `bash -n` exits zero.

- [ ] **Step 4: Commit adapter implementation**

Run:

```bash
git add build/katalyst-agent/bytedance_run.sh
git commit -m "feat(agent): map bulkhead CAT ways parameters"
```

### Task 4: Final verification

**Files:**
- Verify only.

- [ ] **Step 1: Run core focused tests**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm -count=1
go test ./pkg/config/agent/dynamic/adminqos/qrm -count=1
```

Expected: PASS.

- [ ] **Step 2: Run core CAT plugin regression tests**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat -count=1
```

Expected: PASS.

- [ ] **Step 3: Re-run adapter checks**

Run:

```bash
bash -n build/katalyst-agent/bytedance_run.sh
```

Expected: PASS. Record the pre-existing inaccessible Go replacement revision if
`go mod download` remains blocked.

- [ ] **Step 4: Inspect final branch state**

Run in each worktree:

```bash
git status --short --branch
git log -3 --oneline --decorate
```

Expected: clean worktrees on the two requested feature branches.
