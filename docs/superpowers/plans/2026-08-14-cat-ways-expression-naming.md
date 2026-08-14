# CAT Ways Expression Naming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hard-rename the user-facing CAT expression operands from `CBMMask` / `MinCBMBits` to `MaxCATWays` / `MinCATWays` across API, core, tests, generated CRD, documentation, and validation evidence.

**Architecture:** Keep raw Linux resctrl capability names unchanged, but expose only way-count terminology in the expression language. Change the API contract first, then update the core parser and all consumers; reject old operands at both API validation and core parsing boundaries.

**Tech Stack:** Go, Kubernetes `IntOrString`, kubebuilder CEL validation, controller-gen, generated CRD YAML, Go tests, `go vet`, `staticcheck`.

---

## File map

API repository:

- Modify `pkg/apis/config/v1alpha1/bulkhead.go`: public expression constants, comments, and CEL grammar.
- Modify `pkg/apis/config/v1alpha1/adminqos_test.go`: API round-trip fixtures.
- Modify `pkg/apis/config/v1alpha1/bulkhead_schema_test.go`: generated schema assertions and old-token rejection.
- Modify `config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml`: regenerated CRD schema.

Core repository:

- Modify `pkg/config/agent/dynamic/adminqos/qrm/cat_expression.go`: operand kinds, parsing, evaluation names, and evaluator parameter names.
- Modify `pkg/config/agent/dynamic/adminqos/qrm/cat_expression_test.go`: parser/evaluator positive and negative cases.
- Modify `pkg/config/agent/dynamic/adminqos/qrm/rdt_config_test.go`: dynamic configuration fixtures.
- Modify `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`: CLI parsing and apply tests.
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin_test.go`: per-domain expression fixtures.
- Modify CAT expression design and plan documents under `docs/superpowers/`.

Workspace evidence:

- Modify `qrm-bulkhead-test-artifacts/cat-policy-2605-20260814/summary.md`: use the new operands while retaining raw kernel field names.

Raw capability files such as `pkg/util/external/rdt/cat_capability.go` keep
`CBMMask` and `MinCBMBits` because those names model kernel capability data.

---

### Task 1: Rename the API contract

**Files:**

- Modify: `katalyst-api/pkg/apis/config/v1alpha1/bulkhead.go`
- Modify: `katalyst-api/pkg/apis/config/v1alpha1/adminqos_test.go`
- Modify: `katalyst-api/pkg/apis/config/v1alpha1/bulkhead_schema_test.go`
- Modify: `katalyst-api/config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml`

- [ ] **Step 1: Change API tests to the new operand names**

Update API fixtures and expected values:

```go
defaultCATWays := intstr.FromString("MaxCATWays")

ClosCATWays: map[string]intstr.IntOrString{
	"share-00": intstr.FromString("MaxCATWays-MinCATWays"),
	"share-01": intstr.FromInt(2),
}
```

Update schema assertions so their regex accepts only:

```text
MaxCATWays
MinCATWays
positive integer literal
one optional + or - expression
```

Add explicit schema validation cases proving these strings are rejected:

```text
CBMMask
MinCBMBits
CBMMask-MinCBMBits
MaxCATWays-CBMMask
```

- [ ] **Step 2: Run API tests and verify they fail**

Run:

```bash
go test ./pkg/apis/config/v1alpha1 -count=1
```

Expected: failures still reference the old constants and old generated CEL
rules.

- [ ] **Step 3: Rename API constants and CEL grammar**

Replace the expression constants with:

```go
const (
	CATWaysExpressionVariableMaxCATWays CATWaysExpressionVariable = "MaxCATWays"
	CATWaysExpressionVariableMinCATWays CATWaysExpressionVariable = "MinCATWays"
)
```

Update the comments and CEL expressions to use only the new names. Preserve the
existing static rejection rules with the renamed operands:

```text
MaxCATWays-MaxCATWays
MinCATWays-MinCATWays
MinCATWays-MaxCATWays
literal +/- literal
```

Keep raw capability comments precise. For example, change user-facing wording
from “bits from CBMMask” to “ways supported by the domain”; do not rename Linux
`cbm_mask` or `min_cbm_bits`.

- [ ] **Step 4: Regenerate the CRD**

Use the repository's existing generation target:

```bash
make generate
```

If the repository provides a narrower CRD generation target, use it instead and
verify only the expected generated files changed.

Expected: generated CRD CEL rules contain `MaxCATWays` and `MinCATWays`, with no
old expression operand.

- [ ] **Step 5: Run API tests**

Run:

```bash
go test ./pkg/apis/config/v1alpha1 -count=1
git diff --check
```

Expected: PASS and no whitespace errors.

- [ ] **Step 6: Commit the API change**

```bash
git add pkg/apis/config/v1alpha1/bulkhead.go \
  pkg/apis/config/v1alpha1/adminqos_test.go \
  pkg/apis/config/v1alpha1/bulkhead_schema_test.go \
  config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml
git commit -m "feat(config): rename cat ways expression operands"
```

---

### Task 2: Rename the core parser and evaluator

**Files:**

- Modify: `pkg/config/agent/dynamic/adminqos/qrm/cat_expression.go`
- Modify: `pkg/config/agent/dynamic/adminqos/qrm/cat_expression_test.go`

- [ ] **Step 1: Change parser tests to the new vocabulary**

Use positive cases:

```go
{name: "max cat ways", raw: "MaxCATWays", want: "MaxCATWays"},
{name: "min cat ways", raw: "MinCATWays", want: "MinCATWays"},
{name: "subtract variable", raw: "MaxCATWays - MinCATWays", want: "MaxCATWays-MinCATWays"},
{name: "subtract literal", raw: "MaxCATWays-2", want: "MaxCATWays-2"},
{name: "add literal", raw: "MinCATWays+1", want: "MinCATWays+1"},
```

Add all old tokens to the invalid table:

```go
"CBMMask",
"MinCBMBits",
"CBMMask-MinCBMBits",
"MaxCATWays-CBMMask",
"MinCBMBits+1",
```

Rename evaluator fixture fields:

```go
maxCATWays int64
minCATWays int64
```

- [ ] **Step 2: Run parser tests and verify they fail**

Run:

```bash
go test ./pkg/config/agent/dynamic/adminqos/qrm \
  -run 'Test(ParseCATWaysExpression|CATWaysExpressionEvaluate)' -count=1
```

Expected: new operands are rejected and old operands still parse.

- [ ] **Step 3: Implement the hard rename**

Rename operand kinds:

```go
const (
	catWaysOperandInvalid catWaysOperandKind = iota
	catWaysOperandLiteral
	catWaysOperandMaxCATWays
	catWaysOperandMinCATWays
)
```

Parse only the new API constants:

```go
case string(configv1alpha1.CATWaysExpressionVariableMaxCATWays):
	return catWaysOperand{kind: catWaysOperandMaxCATWays, raw: raw}, nil
case string(configv1alpha1.CATWaysExpressionVariableMinCATWays):
	return catWaysOperand{kind: catWaysOperandMinCATWays, raw: raw}, nil
```

Rename evaluator parameters and map operands directly:

```go
func (e CATWaysExpression) Evaluate(maxCATWays, minCATWays int64) (int64, error)

case catWaysOperandMaxCATWays:
	return maxCATWays, nil
case catWaysOperandMinCATWays:
	return minCATWays, nil
```

Update static non-positive checks to compare the renamed operand kinds. Do not
add old-name aliases.

- [ ] **Step 4: Run parser tests**

Run:

```bash
go test ./pkg/config/agent/dynamic/adminqos/qrm \
  -run 'Test(ParseCATWaysExpression|CATWaysExpressionEvaluate)' -count=1
```

Expected: PASS, including hard rejection of old operands.

- [ ] **Step 5: Commit the parser change**

```bash
git add pkg/config/agent/dynamic/adminqos/qrm/cat_expression.go \
  pkg/config/agent/dynamic/adminqos/qrm/cat_expression_test.go
git commit -m "feat(qrm): rename cat ways expression operands"
```

---

### Task 3: Update core integration fixtures

**Files:**

- Modify: `pkg/config/agent/dynamic/adminqos/qrm/rdt_config_test.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin_test.go`

- [ ] **Step 1: Replace integration fixture expressions**

Use:

```text
MaxCATWays
MinCATWays
MaxCATWays-MinCATWays
MaxCATWays-2
MinCATWays+1
```

Keep raw capability struct fields unchanged:

```go
CBMMask:    0x7fff,
MinCBMBits: 1,
```

This distinction is part of the acceptance criteria.

- [ ] **Step 2: Add CLI hard-rejection coverage**

After the new positive CLI case, add a table that applies old expressions
through `CPUPluginOptions.ApplyTo`:

```go
for _, raw := range []string{"CBMMask", "MinCBMBits", "CBMMask-MinCBMBits"} {
	t.Run(raw, func(t *testing.T) {
		options := NewQRMPluginOptions()
		options.BulkheadDefaultCATWays.Value = raw
		config := qrm.NewQRMPluginConfiguration()
		if err := options.ApplyTo(config); err == nil {
			t.Fatalf("ApplyTo accepted legacy expression %q", raw)
		}
	})
}
```

- [ ] **Step 3: Run focused integration tests**

Run:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  ./pkg/config/agent/dynamic/adminqos/qrm \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat \
  -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit integration fixture updates**

```bash
git add cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go \
  pkg/config/agent/dynamic/adminqos/qrm/rdt_config_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin_test.go
git commit -m "test(qrm): cover renamed cat ways operands"
```

---

### Task 4: Update documentation and validation evidence

**Files:**

- Modify: `docs/superpowers/specs/2026-08-13-cat-ways-expression-policy-design.md`
- Modify: `docs/superpowers/plans/2026-08-13-cat-ways-expression-policy.md`
- Modify: `docs/superpowers/specs/2026-08-14-cat-ways-expression-naming-design.md`
- Modify: `docs/superpowers/plans/2026-08-14-cat-ways-expression-naming.md`
- Modify relevant older CAT design references that use the operands as expressions.
- Modify: `qrm-bulkhead-test-artifacts/cat-policy-2605-20260814/summary.md`

- [ ] **Step 1: Rename user-facing expression examples**

Replace expression examples:

```text
CBMMask                    -> MaxCATWays
MinCBMBits                 -> MinCATWays
CBMMask-MinCBMBits         -> MaxCATWays-MinCATWays
```

Do not replace raw kernel or Go capability identifiers:

```text
cbm_mask
min_cbm_bits
CATCapability.CBMMask
CATCapability.MinCBMBits
```

- [ ] **Step 2: Correct the node evidence terminology**

The report must state:

```text
cbm_mask=0x7fff -> MaxCATWays=15
min_cbm_bits=1  -> MinCATWays=1
MaxCATWays-MinCATWays=14
high placement mask=0x7ffe
```

Keep the literal two-way smoke test separate:

```text
dedicated=2 -> 0x6000
```

- [ ] **Step 3: Scan documentation**

Run:

```bash
rg -n 'CBMMask|MinCBMBits' docs/superpowers \
  qrm-bulkhead-test-artifacts/cat-policy-2605-20260814/summary.md
```

Review every remaining match. A remaining match is allowed only when it refers
to raw Go capability fields or explicitly documents that legacy expressions are
invalid.

- [ ] **Step 4: Commit documentation updates**

Stage exact documentation paths, excluding unrelated untracked files:

```bash
git add docs/superpowers/specs/2026-08-13-cat-ways-expression-policy-design.md \
  docs/superpowers/plans/2026-08-13-cat-ways-expression-policy.md \
  docs/superpowers/specs/2026-08-14-cat-ways-expression-naming-design.md \
  docs/superpowers/plans/2026-08-14-cat-ways-expression-naming.md
git commit -m "docs: use cat way count operand names"
```

Commit or preserve the workspace validation report separately according to its
repository ownership.

---

### Task 5: Final verification

**Files:**

- Verify all files changed by Tasks 1-4.

- [ ] **Step 1: Verify the API**

Run in the API repository:

```bash
go test ./pkg/apis/config/v1alpha1 -count=1
git diff --check
```

Expected: PASS.

- [ ] **Step 2: Verify core**

Run in the core repository:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  ./pkg/config/agent/dynamic/adminqos/qrm \
  ./pkg/util/external/rdt \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat \
  -count=1

go vet ./pkg/config/agent/dynamic/adminqos/qrm \
  ./pkg/util/external/rdt \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat

staticcheck ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  ./pkg/config/agent/dynamic/adminqos/qrm \
  ./pkg/util/external/rdt \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat
```

Expected: all commands exit successfully.

- [ ] **Step 3: Verify the hard rename**

Search code and generated schemas:

```bash
rg -n '"CBMMask"|"MinCBMBits"|CBMMask-MinCBMBits' \
  pkg/config/agent/dynamic/adminqos/qrm \
  cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat
```

Expected: matches appear only in negative tests proving old operands are
rejected.

Search API code and generated CRD:

```bash
rg -n 'CBMMask|MinCBMBits' \
  pkg/apis/config/v1alpha1 \
  config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml
```

Expected: no old user-facing operand remains. Any raw capability wording must
refer to kernel fields, not expression tokens.

- [ ] **Step 4: Inspect repository state**

Run:

```bash
git status --short --branch
git log --oneline --decorate -8
```

Expected: only the two known unrelated untracked documents remain in core:

```text
docs/superpowers/plans/2026-08-14-cat-non-overlap-constraints.md
docs/superpowers/specs/2026-08-14-cat-restricted-prefix-selector-design.md
```

Do not stage, modify, or delete either file.
