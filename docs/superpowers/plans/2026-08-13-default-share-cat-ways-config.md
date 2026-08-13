# Default Share CAT Ways Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose bulkhead default and per-CLOS CAT way counts through core flags and adapter environment variables.

**Architecture:** `CPUPluginOptions` owns startup values, tracks whether the scalar CAT flag was explicitly set, and uses pflag's native `StringToInt64Var` to produce `map[string]int64` during flag parsing. katalyst-adapter only maps environment variable names to core flag names; existing dynamic AdminQoSConfiguration application remains the later override.

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
- Modify: adapter `go.mod`

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

- [ ] **Step 5: Lock adapter to the implementing core commit**

After committing the core implementation, compute its pseudo-version from the
UTC commit timestamp and 12-character commit prefix. Update the existing
`github.com/kubewharf/katalyst-core` replacement in adapter `go.mod`, then
commit it separately:

```bash
git add go.mod
git commit -m "build(agent): update katalyst-core revision"
```

The core feature branch must be pushed before resolving this dependency. Do
not use a local filesystem replacement or commit a local path.

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
go list -m -json -mod=readonly github.com/kubewharf/katalyst-core
```

Expected: PASS after the core feature branch is reachable from the replacement
repository. If it has not been published yet, record dependency resolution as
the sole external blocker instead of weakening the dependency contract.

- [ ] **Step 4: Inspect final branch state**

Run in each worktree:

```bash
git status --short --branch
git log -3 --oneline --decorate
```

Expected: clean worktrees on the two requested feature branches.

### Task 5: Review P1 — distinguish omitted default CAT ways from explicit zero

**Files:**
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go`
- Modify: `docs/superpowers/specs/2026-08-13-default-share-cat-ways-config-design.md`
- Modify: `docs/superpowers/plans/2026-08-13-default-share-cat-ways-config.md`

- [x] **Step 1: Write the failing real-parse regression test**

Parse `--bulkhead-default-cat-ways=0` through the registered
`qrm-cpu-plugin` flag set, call `ApplyTo`, and require a lower-case
`bulkhead-default-cat-ways must be positive` error.

- [x] **Step 2: Run the test and verify RED**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  -run '^TestQRMPluginOptions_ParseExplicitZeroBulkheadDefaultCATWays$' -count=1
```

Observed: FAIL with `ApplyTo succeeded, want error`.

- [x] **Step 3: Add minimal explicit-set state**

Keep `BulkheadDefaultCATWays int64`, add one private boolean set bit, and
register a small `pflag.Value` that parses with `strconv.ParseInt` and marks
the bit only after successful parsing. In `ApplyTo`, reject a negative value
as before and reject zero only when the set bit is true.

- [x] **Step 4: Run the regression test and verify GREEN**

Run the Step 2 command again.

Observed: PASS.

- [x] **Step 5: Run final verification and commit atomically**

Run focused tests, the focused package with `-race`, focused `go vet`, gofmt,
and `git diff --check`; inspect the final diff before committing the test,
minimal implementation, design, and plan together without squashing prior
history.

### Task 6: Replace custom flag values with native pflag state

**Files:**
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go`
- Modify: `docs/superpowers/plans/2026-08-13-default-share-cat-ways-config.md`

- [x] **Step 1: Write failing native-type tests**

Update the valid parsing assertion to require:

```go
if !reflect.DeepEqual(options.BulkheadClosCATWays, map[string]int64{"reclaim": 2, "shared": 3}) {
	t.Fatalf("BulkheadClosCATWays = %v, want typed map", options.BulkheadClosCATWays)
}
```

Add a parse-time failure test:

```go
err := fss.FlagSet("qrm-cpu-plugin").Parse(
	[]string{"--bulkhead-clos-cat-ways=reclaim=invalid"},
)
if err == nil {
	t.Fatal("Parse succeeded, want non-integer error")
}
```

Keep the existing real-parse test proving that an omitted scalar accepts the
compatible zero value while explicit `--bulkhead-default-cat-ways=0` fails in
`ApplyTo`.

- [x] **Step 2: Run tests and verify RED**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  -run 'TestQRMPluginOptions_(ParseBulkheadCATWays|ParseInvalidBulkheadClosCATWays|ParseExplicitZeroBulkheadDefaultCATWays)' \
  -count=1
```

Expected: FAIL because `BulkheadClosCATWays` is still `map[string]string` and
non-integer values currently fail in `ApplyTo` instead of flag parsing.

- [x] **Step 3: Use native pflag registrations**

Change the option fields to:

```go
BulkheadDefaultCATWays     int64
bulkheadDefaultCATWaysFlag *pflag.Flag
BulkheadClosCATWays        map[string]int64
```

Register the flags with native pflag values:

```go
fs.Int64Var(&o.BulkheadDefaultCATWays, "bulkhead-default-cat-ways", o.BulkheadDefaultCATWays,
	"default CAT way count for non-root bulkhead CLOS groups.")
o.bulkheadDefaultCATWaysFlag = fs.Lookup("bulkhead-default-cat-ways")
fs.StringToInt64Var(&o.BulkheadClosCATWays, "bulkhead-clos-cat-ways", o.BulkheadClosCATWays,
	"per-CLOS CAT way counts in clos=ways format.")
```

Delete `explicitInt64Value`, its parse-compatibility tests, and the no-longer
needed `strconv` import.

- [x] **Step 4: Simplify `ApplyTo`**

Use pflag's explicit-set state:

```go
defaultCATWaysChanged := o.bulkheadDefaultCATWaysFlag != nil &&
	o.bulkheadDefaultCATWaysFlag.Changed
if o.BulkheadDefaultCATWays < 0 ||
	(defaultCATWaysChanged && o.BulkheadDefaultCATWays == 0) {
	return fmt.Errorf("bulkhead-default-cat-ways must be positive when configured, got %d",
		o.BulkheadDefaultCATWays)
}
```

Validate the typed CLOS map directly:

```go
var closCATWays map[string]int64
if o.BulkheadClosCATWays != nil {
	closCATWays = make(map[string]int64, len(o.BulkheadClosCATWays))
}
for clos, ways := range o.BulkheadClosCATWays {
	if clos == "" {
		return fmt.Errorf("bulkhead-clos-cat-ways contains an empty clos")
	}
	if ways <= 0 {
		return fmt.Errorf("bulkhead-clos-cat-ways value must be positive for clos %q, got %d",
			clos, ways)
	}
	closCATWays[clos] = ways
}
```

Assign the copied `closCATWays` map to the dynamic configuration. This removes
string parsing while preserving the existing no-alias boundary between
options and runtime configuration.

- [x] **Step 5: Run tests and verify GREEN**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm -count=1
go test ./pkg/config/agent/dynamic/adminqos/qrm -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat -count=1
go test -race ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm -count=1
go vet ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm
```

Expected: PASS.

- [x] **Step 6: Format, inspect, and commit**

Run:

```bash
gofmt -w cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go \
  cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go
git diff --check
git diff
git add cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go \
  cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go \
  docs/superpowers/plans/2026-08-13-default-share-cat-ways-config.md
git commit -m "refactor(qrm): use native pflag CAT ways values"
```

### Task 7: Review P2 — preserve explicit state across repeated registration

**Files:**
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go`
- Modify: `docs/superpowers/specs/2026-08-13-default-share-cat-ways-config-design.md`
- Modify: `docs/superpowers/plans/2026-08-13-default-share-cat-ways-config.md`

- [x] **Step 1: Write the failing repeated-registration regression test**

Register the same `CPUPluginOptions` into two separate `NamedFlagSets`, parse
`--bulkhead-default-cat-ways=0` through the first flag set, and require
`ApplyTo` to reject the explicit zero.

- [x] **Step 2: Run the focused test and verify RED**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  -run '^TestCPUPluginOptions_ParseExplicitZeroFromFirstNamedFlagSets$' -count=1
```

Observed: FAIL with `ApplyTo succeeded, want error`.

- [x] **Step 3: Retain every native pflag registration**

Store every registered `*pflag.Flag` in `CPUPluginOptions`. In `ApplyTo`, treat
the scalar as explicitly configured when any retained flag has `Changed=true`.
Keep native `Int64Var`; do not restore a custom `pflag.Value`.

- [x] **Step 4: Run the focused test and verify GREEN**

Run the Step 2 command again.

Observed: PASS.

- [x] **Step 5: Verify, self-review, and commit atomically**

Run the focused package tests, focused race test, focused vet, gofmt, and
`git diff --check`; inspect the final diff, then create a new atomic commit
without amending prior history.

Observed before commit: all commands passed, gofmt produced no diff, and
self-review found no P0-P2 defects.
