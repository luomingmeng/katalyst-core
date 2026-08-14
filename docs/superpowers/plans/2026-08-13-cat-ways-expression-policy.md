# CAT Ways Expression Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. The unchecked boxes below preserve the original execution sequence; the implementation status is recorded separately.

**Status:** Implemented. The task sequence below records the implementation path; the code snippets and behavioral notes have been updated to match the final API and core contracts.

**Goal:** Add AQC-driven dynamic CAT way expressions, `bit_usage` placement constraints, and deterministic allocation groups for RDT CAT bulkhead policy.

**Architecture:** API exposes `IntOrString` CAT way expressions and structured `catPolicy`; core converts the API contract into typed runtime expressions and placement policy. CAT plugin target generation is a deterministic dry-run phase that evaluates expressions per domain, computes an effective placement for each CLOS, packs allocation groups against one shared remaining mask, then applies targets sorted by CLOS ID.

**Tech Stack:** Go, Kubernetes CRD markers, `k8s.io/apimachinery/pkg/util/intstr`, pflag, Linux resctrl CAT files.

---

## File map

API worktree: `/Users/bytedance/go/src/github.com/kubewharf/katalyst-api-cat-ways-expression`

- Modify `pkg/apis/config/v1alpha1/bulkhead.go`: change CAT way fields to `IntOrString`, define expression variable constants, add `CATPolicy`.
- Modify `pkg/apis/config/v1alpha1/adminqos_test.go`: verify integer and string JSON round trips and `catPolicy`.
- Modify `pkg/apis/config/v1alpha1/bulkhead_schema_test.go`: verify int-or-string schema, CEL validation, exact policy enums, required unique group members, and removed unsupported fields.
- Regenerate `pkg/apis/config/v1alpha1/zz_generated.deepcopy.go` and `config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml`.

Core worktree: `/Users/bytedance/go/src/github.com/kubewharf/katalyst-core/.worktrees/default-share-cat-ways-expression`

- Create `pkg/config/agent/dynamic/adminqos/qrm/cat_expression.go`: parse and evaluate CAT way expressions.
- Create `pkg/config/agent/dynamic/adminqos/qrm/cat_expression_test.go`: parser and evaluator tests.
- Modify `pkg/config/agent/dynamic/adminqos/qrm/cpu_plugin.go`: store typed CAT expressions and `CATPolicy`.
- Modify `pkg/config/agent/dynamic/adminqos/qrm/rdt_config_test.go`: verify AQC `IntOrString` conversion.
- Modify `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go`: change CAT options to string boundary and parse expressions in `ApplyTo`.
- Modify `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`: update flag tests for integer compatibility and expression values.
- Modify `pkg/util/external/rdt/cat_capability.go`: read optional `info/L3/bit_usage`.
- Modify `pkg/util/external/rdt/cat_capability_test.go`: test `bit_usage` parsing and missing-file compatibility.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin.go`: deterministic expression target generation and allocation group packing.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin_test.go`: preserve old behavior and cover expression, placement, packing, and stability.

## Task 1: API CAT expression contract

**Files:**
- Modify: `/Users/bytedance/go/src/github.com/kubewharf/katalyst-api-cat-ways-expression/pkg/apis/config/v1alpha1/bulkhead.go`
- Modify: `/Users/bytedance/go/src/github.com/kubewharf/katalyst-api-cat-ways-expression/pkg/apis/config/v1alpha1/adminqos_test.go`

- [ ] **Step 1: Add failing API round-trip tests**

Update `TestQRMPluginConfigRDTAndBulkheadRDTConfig` to use:

```go
defaultCATWays := intstr.FromString("MaxCATWays")
config := QRMPluginConfig{
    RDTConfig: &RDTConfig{DisableRDT: &disableRDT},
    CPUPluginConfig: &CPUPluginConfig{
        BulkheadConfig: &BulkheadConfig{
            BulkheadRDTConfig: &BulkheadRDTConfig{
                EnableCPUList:  &enableCPUList,
                EnableCAT:      &enableCAT,
                DefaultCATWays: &defaultCATWays,
                ClosCATWays: map[string]intstr.IntOrString{
                    "share-00": intstr.FromString("MaxCATWays-MinCATWays"),
                    "share-01": intstr.FromInt(2),
                },
                CATPolicy: &CATPolicy{
                    AllocationGroups: []CATAllocationGroup{{
                        Name:             "gpu-safe-shared",
                        ClosIDs:          []string{"share-00", "share-01"},
                        AllowedBitUsages: []CATBitUsage{CATBitUsageSoftware},
                        Direction:        CATAllocationDirectionLow,
                    }},
                },
            },
        },
    },
}
```

Assert after unmarshal:

```go
if bulkheadRDTConfig.DefaultCATWays == nil || bulkheadRDTConfig.DefaultCATWays.String() != "MaxCATWays" {
    t.Fatalf("DefaultCATWays = %v, want MaxCATWays", bulkheadRDTConfig.DefaultCATWays)
}
if got := bulkheadRDTConfig.ClosCATWays["share-00"].String(); got != "MaxCATWays-MinCATWays" {
    t.Fatalf("ClosCATWays[share-00] = %s, want MaxCATWays-MinCATWays", got)
}
if got := bulkheadRDTConfig.ClosCATWays["share-01"].IntValue(); got != 2 {
    t.Fatalf("ClosCATWays[share-01] = %d, want 2", got)
}
if got := bulkheadRDTConfig.CATPolicy.AllocationGroups[0].AllowedBitUsages[0]; got != CATBitUsageSoftware {
    t.Fatalf("AllowedBitUsages[0] = %s, want %s", got, CATBitUsageSoftware)
}
```

- [ ] **Step 2: Run API test and verify it fails**

Run:

```bash
go test ./pkg/apis/config/v1alpha1 -run TestQRMPluginConfigRDTAndBulkheadRDTConfig -count=1
```

Expected: compile failure for missing `intstr` import, `CATPolicy`, `CATBitUsage`, or type mismatch on `DefaultCATWays`.

- [ ] **Step 3: Update API types**

In `bulkhead.go`, import:

```go
import "k8s.io/apimachinery/pkg/util/intstr"
```

Change fields and add types:

```go
// +kubebuilder:validation:XValidation:rule="!has(self.enableCAT) || !self.enableCAT || has(self.defaultCATWays)",message="defaultCATWays must be specified when enableCAT is true"
type BulkheadRDTConfig struct {
    EnableCPUList *bool `json:"enableCPUList,omitempty"`
    EnableCAT     *bool `json:"enableCAT,omitempty"`
    // DefaultCATWays is the default CAT way count or expression for non-root CLOS.
    // Supported expression variables are MaxCATWays and MinCATWays.
    // +optional
    // +kubebuilder:validation:XIntOrString
    DefaultCATWays *intstr.IntOrString `json:"defaultCATWays,omitempty"`
    // ClosCATWays maps pool names or CLOS IDs to CAT way counts or expressions.
    // +optional
    ClosCATWays map[string]intstr.IntOrString `json:"closCATWays,omitempty"`
    // CATPolicy controls CAT bit placement and deterministic allocation groups.
    // +optional
    CATPolicy *CATPolicy `json:"catPolicy,omitempty"`
}

type CATWaysExpressionVariable string

const (
    CATWaysExpressionVariableMaxCATWays CATWaysExpressionVariable = "MaxCATWays"
    CATWaysExpressionVariableMinCATWays CATWaysExpressionVariable = "MinCATWays"
)

type CATPolicy struct {
    DefaultPlacement *CATPlacementPolicy           `json:"defaultPlacement,omitempty"`
    ClosPlacements   map[string]CATPlacementPolicy `json:"closPlacements,omitempty"`
    AllocationGroups []CATAllocationGroup          `json:"allocationGroups,omitempty"`
}

type CATPlacementPolicy struct {
    AllowedBitUsages []CATBitUsage          `json:"allowedBitUsages,omitempty"`
    Direction        CATAllocationDirection `json:"direction,omitempty"`
}

type CATAllocationGroup struct {
    Name             string                 `json:"name,omitempty"`
    ClosIDs          []string               `json:"closIDs,omitempty"`
    AllowedBitUsages []CATBitUsage          `json:"allowedBitUsages,omitempty"`
    Direction        CATAllocationDirection `json:"direction,omitempty"`
}

type CATBitUsage string

const (
    CATBitUsageSoftware  CATBitUsage = "S"
    CATBitUsageHardware  CATBitUsage = "H"
    CATBitUsageExclusive CATBitUsage = "X"
)

type CATAllocationDirection string

const (
    CATAllocationDirectionLow  CATAllocationDirection = "low"
    CATAllocationDirectionHigh CATAllocationDirection = "high"
)

```

- [ ] **Step 4: Run API round-trip test**

Run:

```bash
go test ./pkg/apis/config/v1alpha1 -run TestQRMPluginConfigRDTAndBulkheadRDTConfig -count=1
```

Expected: PASS or fail only because deepcopy/generated CRD tests are stale.

- [ ] **Step 5: Commit API type change**

```bash
git add pkg/apis/config/v1alpha1/bulkhead.go pkg/apis/config/v1alpha1/adminqos_test.go
git commit -m "feat(config): add cat ways expression api"
```

## Task 2: API generated artifacts and schema tests

**Files:**
- Modify: `/Users/bytedance/go/src/github.com/kubewharf/katalyst-api-cat-ways-expression/pkg/apis/config/v1alpha1/bulkhead_schema_test.go`
- Modify generated: `/Users/bytedance/go/src/github.com/kubewharf/katalyst-api-cat-ways-expression/pkg/apis/config/v1alpha1/zz_generated.deepcopy.go`
- Modify generated: `/Users/bytedance/go/src/github.com/kubewharf/katalyst-api-cat-ways-expression/config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml`

- [ ] **Step 1: Update schema test expectations**

Replace the integer minimum expectations with checks for `XIntOrString` JSON fields and policy properties:

```go
type jsonSchema struct {
    Properties             map[string]jsonSchema `json:"properties"`
    Minimum                *float64              `json:"minimum"`
    Type                   string                `json:"type"`
    XIntOrString           bool                  `json:"x-kubernetes-int-or-string"`
    XValidations           []validationRule      `json:"x-kubernetes-validations"`
    XKubernetesValidations []validationRule      `json:"x-kubernetes-validations"`
    Enum                   []string              `json:"enum"`
    Items                  *jsonSchema           `json:"items"`
    AdditionalProperties   *jsonSchema           `json:"additionalProperties"`
}
```

Assert:

```go
if !schemaProperty(t, schema, "defaultCATWays").XIntOrString {
    t.Fatal("defaultCATWays is not x-kubernetes-int-or-string")
}
closCATWays := schemaProperty(t, schema, "closCATWays")
if closCATWays.AdditionalProperties == nil || !closCATWays.AdditionalProperties.XIntOrString {
    t.Fatal("closCATWays values are not x-kubernetes-int-or-string")
}
catPolicy := schemaProperty(t, schema, "catPolicy")
allocationGroups := schemaProperty(t, catPolicy, "allocationGroups")
groupItem := allocationGroups.Items
if groupItem == nil {
    t.Fatal("allocationGroups has no item schema")
}
```

- [ ] **Step 2: Regenerate API artifacts**

Run:

```bash
make generate-manifests generate-go
```

Expected: CRD and deepcopy files update.

- [ ] **Step 3: Run focused API tests**

Run:

```bash
go test ./pkg/apis/config/v1alpha1 -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit generated API artifacts**

```bash
git add pkg/apis/config/v1alpha1 config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml
git commit -m "feat(config): generate cat policy crd"
```

## Task 3: Core expression parser

**Files:**
- Create: `/Users/bytedance/go/src/github.com/kubewharf/katalyst-core/.worktrees/default-share-cat-ways-expression/pkg/config/agent/dynamic/adminqos/qrm/cat_expression.go`
- Create: `/Users/bytedance/go/src/github.com/kubewharf/katalyst-core/.worktrees/default-share-cat-ways-expression/pkg/config/agent/dynamic/adminqos/qrm/cat_expression_test.go`

- [ ] **Step 1: Write failing parser tests**

Create tests for:

```go
func TestParseCATWaysExpression(t *testing.T) {
    cases := []struct {
        name string
        raw  string
        want string
    }{
        {"literal", "4", "4"},
        {"max cat ways", "MaxCATWays", "MaxCATWays"},
        {"min cat ways", "MinCATWays", "MinCATWays"},
        {"subtract variable", "MaxCATWays - MinCATWays", "MaxCATWays-MinCATWays"},
        {"subtract literal", "MaxCATWays-2", "MaxCATWays-2"},
        {"add literal", "MinCATWays+1", "MinCATWays+1"},
    }
    for _, tt := range cases {
        t.Run(tt.name, func(t *testing.T) {
            got, err := ParseCATWaysExpression(tt.raw)
            require.NoError(t, err)
            require.Equal(t, tt.want, got.String())
        })
    }
}
```

Add invalid cases for `""`, `"0"`, `"-1"`, `"MaxCATWays/2"`, `"MaxCATWays-MinCATWays-1"`, `"cbm_mask"`, operand-internal whitespace, literal-only arithmetic, and statically non-positive expressions. Add explicit legacy-invalid cases for `"CBMMask"`, `"MinCBMBits"`, and `"CBMMask-MinCBMBits"`.

- [ ] **Step 2: Run parser test and verify it fails**

Run:

```bash
go test ./pkg/config/agent/dynamic/adminqos/qrm -run TestParseCATWaysExpression -count=1
```

Expected: compile failure for undefined `ParseCATWaysExpression`.

- [ ] **Step 3: Implement parser and evaluator**

Implement public functions:

```go
func ParseCATWaysExpression(raw string) (CATWaysExpression, error)
func (e CATWaysExpression) Evaluate(maxWays int64, minCBMBits int64) (int64, error)
func (e CATWaysExpression) String() string
func (e CATWaysExpression) Configured() bool
```

Use an internal enum for operands: literal, max ways, and min CBM bits. The production package does not expose a panic-based parse helper; tests use a local helper around `ParseCATWaysExpression`.

- [ ] **Step 4: Run parser tests**

Run:

```bash
go test ./pkg/config/agent/dynamic/adminqos/qrm -run 'TestParseCATWaysExpression|TestCATWaysExpressionEvaluate' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit parser**

```bash
git add pkg/config/agent/dynamic/adminqos/qrm/cat_expression.go pkg/config/agent/dynamic/adminqos/qrm/cat_expression_test.go
git commit -m "feat(qrm): parse cat ways expressions"
```

## Task 4: Core config and options conversion

**Files:**
- Modify: `pkg/config/agent/dynamic/adminqos/qrm/cpu_plugin.go`
- Modify: `pkg/config/agent/dynamic/adminqos/qrm/rdt_config_test.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`

- [ ] **Step 1: Update failing config and option tests**

Update tests so `DefaultCATWays` and `ClosCATWays` use `CATWaysExpression`. Add flag parse coverage:

```go
args := []string{
    "--bulkhead-default-cat-ways=MaxCATWays",
    "--bulkhead-clos-cat-ways=share-00=MaxCATWays-MinCATWays,share-01=2",
}
```

Assert:

```go
if got := rdt.DefaultCATWays.String(); got != "MaxCATWays" {
    t.Fatalf("DefaultCATWays = %s, want MaxCATWays", got)
}
if got := rdt.ClosCATWays["share-00"].String(); got != "MaxCATWays-MinCATWays" {
    t.Fatalf("ClosCATWays[share-00] = %s, want MaxCATWays-MinCATWays", got)
}
```

- [ ] **Step 2: Run focused core config tests and verify failure**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm ./pkg/config/agent/dynamic/adminqos/qrm -count=1
```

Expected: compile failures from old integer types.

- [ ] **Step 3: Change dynamic config and options types**

Change:

```go
DefaultCATWays CATWaysExpression
ClosCATWays    map[string]CATWaysExpression
```

Change options:

```go
BulkheadDefaultCATWays utilflag.ExplicitValue[string]
BulkheadClosCATWays    map[string]string
```

Change flags:

```go
fs.StringVar(&o.BulkheadDefaultCATWays.Value, "bulkhead-default-cat-ways", o.BulkheadDefaultCATWays.Value,
    "default CAT way count expression for non-root bulkhead CLOS groups.")
fs.StringToStringVar(&o.BulkheadClosCATWays, "bulkhead-clos-cat-ways", o.BulkheadClosCATWays,
    "per-CLOS CAT way count expressions in clos=expression format.")
```

Parse in `ApplyTo` using `ParseCATWaysExpression`.

- [ ] **Step 4: Convert AQC IntOrString to expression**

In `ApplyConfiguration`, convert API values through helper functions:

```go
func ParseCATWaysExpressionFromIntOrString(value intstr.IntOrString) (CATWaysExpression, error)
```

Because `ApplyConfiguration` does not return an error, conversion stores a descriptive `CATConfigError`. The CAT plugin checks this field before reading capabilities or writing resctrl. A failed parse leaves the previous typed value unchanged but blocks CAT application until the dynamic configuration is valid:

```go
func applyCATWaysExpression(dst *CATWaysExpression, value intstr.IntOrString) error {
    expr, err := ParseCATWaysExpressionFromIntOrString(value)
    if err != nil {
        return err
    }
    *dst = expr
    return nil
}
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm ./pkg/config/agent/dynamic/adminqos/qrm -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit config conversion**

```bash
git add cmd/katalyst-agent/app/options/dynamic/adminqos/qrm pkg/config/agent/dynamic/adminqos/qrm
git commit -m "feat(qrm): wire cat expression config"
```

## Task 5: CAT capability bit usage

**Files:**
- Modify: `pkg/util/external/rdt/cat_capability.go`
- Modify: `pkg/util/external/rdt/cat_capability_test.go`

- [ ] **Step 1: Add failing bit_usage tests**

Add a test with:

```go
require.NoError(t, os.WriteFile(filepath.Join(root, "info", "L3", "bit_usage"), []byte("0=XXSS;1=SSXX\n"), 0o644))
```

Assert `BitUsageByType["S"]` and `BitUsageByType["X"]` masks for each domain. The test name must document whether the rightmost character maps to bit 0.

- [ ] **Step 2: Run capability tests and verify failure**

Run:

```bash
go test ./pkg/util/external/rdt -run CATCapability -count=1
```

Expected: compile failure or zero `BitUsageByType`.

- [ ] **Step 3: Implement optional bit_usage parsing**

Extend `CATCapability`:

```go
BitUsageByType map[string]uint64
```

Read `info/L3/bit_usage` if present. Missing file returns capabilities without bit usage. Malformed existing file returns an error.

- [ ] **Step 4: Run capability tests**

Run:

```bash
go test ./pkg/util/external/rdt -run CATCapability -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit bit_usage support**

```bash
git add pkg/util/external/rdt/cat_capability.go pkg/util/external/rdt/cat_capability_test.go
git commit -m "feat(rdt): read cat bit usage"
```

## Task 6: CAT plugin deterministic target generation

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin_test.go`

- [ ] **Step 1: Add failing plugin tests**

Add tests for:

```go
func TestCATPluginBuildsExpressionTargets(t *testing.T)
func TestCATPluginAllocationGroupPacksDeterministicallyInSRegion(t *testing.T)
func TestCATPluginAllocationGroupUsesPerCLOSPlacementWithoutOverlap(t *testing.T)
func TestCATPluginAllocationGroupRejectsUnmanagedCLOS(t *testing.T)
func TestCATPluginAllocationGroupRejectsInsufficientRemainingWays(t *testing.T)
func TestCATPluginRejectsAllWritesWhenAnyDomainCannotSatisfyWays(t *testing.T)
```

Use capabilities with two domains and bit usage. Assert exact masks and exact write order.

- [ ] **Step 2: Run plugin tests and verify failure**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat -count=1
```

Expected: compile failures due to changed config types and missing packing implementation.

- [ ] **Step 3: Refactor target building**

Replace `reconcile(ctx, defaultWays int64, overrides map[string]int64)` with expression config input. Add:

```go
func buildTargets(
    capabilities map[int]rdt.CATCapability,
    clos []qrmresctrlmanager.CPUListClos,
    defaultExpr qrm.CATWaysExpression,
    overrides map[string]qrm.CATWaysExpression,
    policy qrm.CATPolicy,
) ([]catTarget, error)
```

Sort domains ascending and sort final `catTarget` by CLOS ID before apply.

- [ ] **Step 4: Implement placement and group packing**

Implement:

```go
func contiguousMaskWithDirection(available uint64, ways int, direction CATAllocationDirection) (uint64, bool)
func allowedMask(capability rdt.CATCapability, usages []CATBitUsage) (uint64, error)
func effectiveGroupMemberPlacement(group CATAllocationGroup, override CATPlacementPolicy) CATPlacementPolicy
func packAllocationGroup(group CATAllocationGroup, domains []int, capabilities map[int]rdt.CATCapability, expressions map[string]qrm.CATWaysExpression, closPlacements map[string]CATPlacementPolicy, managed map[string]struct{}, targets map[string]map[int]uint64, grouped map[string]struct{}) error
```

Group packing initializes `remaining` from the full domain `CBMMask`. Each member merges non-empty `closPlacements` fields over the group default, filters its candidate as `remaining & allowedMask`, then deducts `remaining &^= mask` in explicit `closIDs` order.

- [ ] **Step 5: Run plugin tests**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit plugin implementation**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat
git commit -m "feat(qrm): allocate cat ways by policy"
```

## Task 7: Full focused verification

**Files:**
- Verify all modified API and core files.

- [ ] **Step 1: Run API verification**

Run:

```bash
go test ./pkg/apis/config/v1alpha1 -count=1
```

Expected: PASS.

- [ ] **Step 2: Run core verification**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm ./pkg/config/agent/dynamic/adminqos/qrm ./pkg/util/external/rdt ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat -count=1
```

Expected: PASS.

- [ ] **Step 3: Inspect final diffs**

Run in both worktrees:

```bash
git status --short --branch
git log --oneline --decorate -5
```

Expected: clean worktrees, atomic commits for API and core.
