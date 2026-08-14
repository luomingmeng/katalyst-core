# CAT ways expression and placement policy design

## Background

The current RDT CAT bulkhead configuration supports a fixed way count through `defaultCATWays` and per-CLOS overrides through `closCATWays`. That model works when every target CLOS can use the same contiguous portion of the L3 CAT `CBMMask`, but it cannot express node-dependent policies such as "use the full hardware CAT capacity", "use `MaxCATWays-MinCATWays`", or "allocate only from the software-owned `S` area in `/sys/fs/resctrl/info/L3/bit_usage`".

The new policy must support dynamic way count expressions, CAT bit usage placement constraints, and stable non-overlapping allocation across multiple CLOS IDs. The result must be configurable through `AdminQoSConfiguration` and must remain deterministic across repeated reconciliations, agent restarts, map iteration changes, and CLOS list ordering differences.

## Baseline before implementation

Before this feature, API defined `BulkheadRDTConfig` in `pkg/apis/config/v1alpha1/bulkhead.go` with `DefaultCATWays *int64` and `ClosCATWays map[string]int64`. The generated CRD enforced `defaultCATWays >= 1`, `closCATWays` values `> 0`, and the CEL rule that `defaultCATWays` must be present when `enableCAT=true`.

Core dynamic configuration mirrored that integer model in `pkg/config/agent/dynamic/adminqos/qrm/cpu_plugin.go`, and static flag parsing used an integer flag for `--bulkhead-default-cat-ways` plus `StringToInt64Var` for `--bulkhead-clos-cat-ways`.

The CAT plugin resolved per-CLOS integer overrides, built symmetric targets, and chose the first contiguous low-bit mask. It already built all targets before applying CAT writes, but CLOS apply order, domain map iteration, and group allocation order were not explicit.

The CAT capability provider in `pkg/util/external/rdt/cat_capability.go` reads `info/L3/cbm_mask`, `info/L3/min_cbm_bits`, and root `schemata` to discover L3 domains. It does not read `info/L3/bit_usage`. `SchemataCoordinator.ApplyL3` formats per-domain masks and skips repeated writes through a cache; that cache must remain an optimization and must not become an input to allocation decisions.

`pkg/util/resctrl/clos.go` already has `ResolveCATWayKey`, which maps pool names through `CPUSetPoolToSharedSubgroup` and normalizes CLOS IDs. The same resolver must be reused by `closCATWays`, `catPolicy.closPlacements`, and `catPolicy.allocationGroups` so that one AQC key always maps to one canonical CLOS ID.

## Goals

The design supports these AQC-level capabilities:

- `defaultCATWays` and `closCATWays` accept either a positive integer or a dynamic way count expression.
- Expressions can refer to per-domain `MaxCATWays` and `MinCATWays`.
- A CLOS can restrict candidate CAT bits by `bit_usage` class, for example only `S`.
- A CLOS can choose a stable allocation direction, initially `low` or `high`.
- A group of CLOS IDs is packed into a shared allowed region without overlap.
- Repeated reconciliations over the same AQC and hardware inputs produce the same mask for every CLOS and domain.
- Existing positive integer literal AQC and CLI configurations keep their behavior.

## Non-goals

The first implementation will not support arbitrary arithmetic, bitwise mask expressions, direct hexadecimal mask literals, CDP `L3CODE`/`L3DATA`, or automatic interpretation of current CLOS `schemata` as allocation state. Allocation is recomputed from desired configuration and hardware capabilities on every reconcile.

## API model

`BulkheadRDTConfig` keeps the existing field names and adds a structured `catPolicy` block:

```go
type BulkheadRDTConfig struct {
    EnableCPUList *bool `json:"enableCPUList,omitempty"`
    EnableCAT     *bool `json:"enableCAT,omitempty"`

    DefaultCATWays *intstr.IntOrString          `json:"defaultCATWays,omitempty"`
    ClosCATWays    map[string]intstr.IntOrString `json:"closCATWays,omitempty"`

    CATPolicy *CATPolicy `json:"catPolicy,omitempty"`
}
```

`DefaultCATWays` and `ClosCATWays` still mean "way count", not final CBM mask. Existing positive integer literal YAML remains valid and keeps its behavior:

```yaml
defaultCATWays: 4
closCATWays:
  reclaim: 2
```

String expressions become valid:

```yaml
defaultCATWays: MaxCATWays
closCATWays:
  share-00: MaxCATWays-MinCATWays
  share-01: MinCATWays
  share-02: MaxCATWays-2
```

The new policy types are:

```go
type CATPolicy struct {
    DefaultPlacement *CATPlacementPolicy          `json:"defaultPlacement,omitempty"`
    ClosPlacements   map[string]CATPlacementPolicy `json:"closPlacements,omitempty"`
    AllocationGroups []CATAllocationGroup        `json:"allocationGroups,omitempty"`
}

type CATPlacementPolicy struct {
    AllowedBitUsages []CATBitUsage           `json:"allowedBitUsages,omitempty"`
    Direction        CATAllocationDirection  `json:"direction,omitempty"`
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

The expression variable names should also be defined in the API package so CRD comments, examples, tests, and core conversion code share one public contract instead of duplicating string literals:

```go
type CATWaysExpressionVariable string

const (
    CATWaysExpressionVariableMaxCATWays CATWaysExpressionVariable = "MaxCATWays"
    CATWaysExpressionVariableMinCATWays CATWaysExpressionVariable = "MinCATWays"
)
```

These constants are not standalone CRD fields. They define the supported symbolic operands inside `defaultCATWays` and `closCATWays` string expressions. Core may convert them into an internal parser enum, but it should treat the API constants as the external spelling contract.

`AllocationGroups` is the primary mechanism for stable non-overlapping packing. A CLOS may appear in at most one allocation group after key resolution. If a CLOS appears in a group, the group supplies the default member placement and the shared remaining mask used to prevent overlap. `closPlacements` still applies to group members and overrides non-empty placement fields for that CLOS. This is required when group members must not overlap but do not share the same `bit_usage` constraints.

## AQC examples

This example keeps three CLOS IDs in one non-overlapping allocation group while allowing `share-02` to use `X` bits. `share-00` and `share-01` inherit the group default `S`-only placement. `share-02` overrides its member placement through `closPlacements`, so it may use either `S` or `X`, but it still shares the group remaining mask and cannot overlap with the earlier CLOS IDs.

```yaml
bulkheadRDTConfig:
  enableCAT: true
  defaultCATWays: MaxCATWays
  closCATWays:
    share-00: MinCATWays
    share-01: MinCATWays
    share-02: 4
  catPolicy:
    allocationGroups:
      - name: gpu-safe-shared
        closIDs: ["share-00", "share-01", "share-02"]
        allowedBitUsages: ["S"]
        direction: low
    closPlacements:
      share-02:
        allowedBitUsages: ["S", "X"]
        direction: high
```

For a node whose `bit_usage` is:

```text
0=XXSSSSSSSSSSSSS;1=XXSSSSSSSSSSSSS
```

the group-level `allowedBitUsages: ["S"]` excludes the `X` bits from the candidate mask for `share-00` and `share-01`. For `share-02`, `closPlacements.share-02.allowedBitUsages: ["S", "X"]` overrides the group default and makes both `S` and `X` candidates. The group still allocates in the explicit `closIDs` order and deducts each selected mask from the same remaining mask before processing the next CLOS, so `share-02` cannot overlap with `share-00` or `share-01`.

Independent placement remains possible without group packing:

```yaml
catPolicy:
  defaultPlacement:
    allowedBitUsages: ["S"]
    direction: low
  closPlacements:
    dedicated:
      allowedBitUsages: ["S"]
      direction: high
```

If `catPolicy` is omitted, `closCATWays` only controls the way count expression for each CLOS. The plugin uses the default independent placement semantics: it selects a contiguous mask from the full `CBMMask`, scans from low bits first, does not filter by `bit_usage`, and does not reserve bits between CLOS IDs. Existing AQC objects that use positive integer literals in `defaultCATWays` or `closCATWays` therefore keep the same masks. Symbolic expressions use the new operand contract below and are not covered by this compatibility guarantee.

The following AQC uses dynamic way expressions with default independent placement:

```yaml
bulkheadRDTConfig:
  enableCAT: true
  # Without catPolicy, MaxCATWays only means the number of available CAT ways.
  # It does not restrict allocation to S/H/X regions from bit_usage.
  defaultCATWays: MaxCATWays
  closCATWays:
    # These values decide how many ways each CLOS gets.
    # They do not reserve ways against each other unless catPolicy.allocationGroups is configured.
    share-00: MinCATWays
    share-01: MaxCATWays-2
```

## Expression grammar

The expression grammar is intentionally small:

```text
expr    := operand | operand op operand
operand := positive_integer | "MaxCATWays" | "MinCATWays"
op      := "+" | "-"
```

Binary expressions must contain at least one symbolic operand. Literal-only arithmetic such as `1+2` or `4-1` is rejected and should be written in simplified form. Expressions that are statically non-positive are also rejected, including `MaxCATWays-MaxCATWays`, `MinCATWays-MinCATWays`, and `MinCATWays-MaxCATWays`.

`MaxCATWays` means `bits.OnesCount64(capability.CBMMask)` for the current L3 domain. It does not mean the hexadecimal mask value. `MinCATWays` means `capability.MinCBMBits` for the current L3 domain. The Go capability fields remain `CBMMask` and `MinCBMBits` because they model raw resctrl capability data.

Supported examples:

```text
4
MaxCATWays
MinCATWays
MaxCATWays-MinCATWays
MaxCATWays-2
MinCATWays+1
```

Unsupported examples:

```text
MaxCATWays-MinCATWays-1
MaxCATWays/2
0xff
(MaxCATWays-2)
cbm_mask
1+2
MaxCATWays-MaxCATWays
```

Legacy operand spellings are explicitly invalid:

```text
CBMMask
MinCBMBits
CBMMask-MinCBMBits
```

Compatibility is limited to positive integer literals. Neither the API nor core aliases the legacy symbolic operands: AQC validation and the core parser must hard-reject `CBMMask`, `MinCBMBits`, and every expression containing either spelling. Operators must migrate configurations before rollout:

```text
CBMMask             -> MaxCATWays
MinCBMBits           -> MinCATWays
CBMMask-MinCBMBits  -> MaxCATWays-MinCATWays
```

This is an intentional breaking change for the earlier symbolic-expression contract. Silent aliasing is forbidden because it would keep ambiguous mask-oriented names alive and make persisted AQC behavior depend on the binary version that reads it.

Parser output keeps a canonical representation, so `MaxCATWays - 2` and `MaxCATWays-2` compare equal. Leading and trailing whitespace and whitespace around the operator are accepted; whitespace inside an operand is rejected. Literal integer values supplied through `IntOrString` canonicalize the same way as string integer values.

API should expose `MaxCATWays` and `MinCATWays` as `CATWaysExpressionVariable` constants. The core parser should accept only those API-defined spellings for symbolic operands, while still storing its runtime representation as an internal enum for efficient evaluation.

## Core internal model

Core should not store raw `intstr.IntOrString` in runtime configuration. The boundary conversion should produce a typed expression and typed policy:

```go
type CATWaysExpression struct {
    Raw        string
    Configured bool
    LHS        CATWaysOperand
    Operator   CATWaysOperator
    RHS        CATWaysOperand
}

type CATWaysOperand struct {
    Kind  CATWaysOperandKind
    Value int64
}
```

The dynamic RDT config becomes:

```go
type DynamicBulkheadRDTConfiguration struct {
    EnableCPUList  bool
    EnableCAT      bool
    DefaultCATWays CATWaysExpression
    ClosCATWays    map[string]CATWaysExpression
    CATPolicy      CATPolicy
}
```

`Configured` preserves the existing zero-value behavior: when CAT is disabled or rollback uses an unconfigured default, the plugin restores the full capability mask. When `enableCAT=true`, API validation still requires `defaultCATWays`, so normal CAT operation should always have a configured default expression.

## CLI and environment compatibility

The CLI boundary should change from integer to string:

```go
BulkheadDefaultCATWays utilflag.ExplicitValue[string]
BulkheadClosCATWays    map[string]string
```

The flag names remain unchanged:

```bash
--bulkhead-default-cat-ways=MaxCATWays
--bulkhead-clos-cat-ways=share-00=MinCATWays,share-01=MaxCATWays-2
```

Existing positive integer literal invocations continue to work unchanged:

```bash
--bulkhead-default-cat-ways=4
--bulkhead-clos-cat-ways=reclaim=2,shared=3
```

CLI values containing the legacy `CBMMask` or `MinCBMBits` operands fail parsing and must be migrated to `MaxCATWays` or `MinCATWays`. The CLI does not provide a legacy-token fallback.

The adapter build script already maps `QRMCPUPluginBulkheadDefaultCATWays` to `bulkhead-default-cat-ways` and `QRMCPUPluginBulkheadClosCATWays` to `bulkhead-clos-cat-ways`. Those environment variables should remain plain string pass-throughs. `catPolicy` is structured and should be delivered through AQC rather than CLI flags.

## CAT capability and bit usage

`CATCapability` should be extended:

```go
type CATCapability struct {
    CBMMask        uint64
    MinCBMBits     int
    BitUsageByType map[string]uint64
}
```

The capability provider reads `info/L3/bit_usage` when present. Missing `bit_usage` should not make CAT unsupported unless the active policy requires `allowedBitUsages`. This keeps existing behavior on nodes or kernels that do not expose `bit_usage`.

The parser must handle lines like:

```text
0=XXSSSSSSSSSSSSS;1=XXSSSSSSSSSSSSS
```

The implementation must verify the bit order against Linux resctrl semantics. The intended internal contract is:

- `direction: low` scans from numeric bit 0 upward.
- `direction: high` scans from the highest set bit downward.
- `bit_usage` characters are converted into numeric bit masks before allocation.

Tests must fix the mapping. For the example above, if the rightmost character maps to bit 0, then the leading `XX` maps to the highest CAT bits. If the kernel uses a different representation, the parser must encode that exact representation and document it in the test name.

## Placement resolution

For a CLOS outside an allocation group, placement starts from `defaultPlacement`. A matching `closPlacements[closID]` replaces it. If neither exists, empty placement means no `bit_usage` filtering and low-direction selection.

For a CLOS inside an allocation group, the group fields provide the member default. Non-empty fields from `closPlacements[closID]` override the corresponding group fields independently: a non-empty `allowedBitUsages` slice replaces the group slice, and a non-empty direction replaces the group direction. `defaultPlacement` does not apply to group members.

The available mask for a CLOS/domain is:

```go
available := capability.CBMMask
if len(placement.AllowedBitUsages) > 0 {
    allowedByUsage := unionBitUsageMasks(capability.BitUsageByType, placement.AllowedBitUsages)
    available &= allowedByUsage
}
```

If `allowedBitUsages` is configured and a required bit usage symbol is absent from `bit_usage`, allocation fails with a configuration error for that domain.

## Deterministic allocation

Reconciliation must be a pure function of AQC, managed CLOS IDs, resctrl config, and hardware capabilities. It must not read current CLOS `schemata` to decide which bits to allocate.

The reconcile flow should be:

1. Read CAT capabilities, including optional `bit_usage`.
2. List managed CLOS IDs.
3. Resolve all `closCATWays`, `closPlacements`, and `allocationGroups[].closIDs` with `ResolveCATWayKey`.
4. Validate that no resolved CLOS appears in multiple allocation groups.
5. Build target masks for every managed CLOS without writing resctrl.
6. Validate all group packing and target constraints.
7. Sort apply targets by CLOS ID and apply them.
8. On apply failure, rollback using the configured default expression if present, otherwise full capability mask.

Domain iteration must sort domain IDs. CLOS apply order must sort CLOS IDs, except allocation group packing uses the explicit `closIDs` order only while calculating masks. Map iteration must never affect target masks or write order.

## Allocation group algorithm

For each allocation group, initialize one remaining mask per domain from the full `CBMMask`. Then process members in `closIDs` order:

```go
remaining := capability.CBMMask

for _, closID := range group.closIDs {
    placement := groupDefaultPlacement(group)
    placement = mergeNonEmptyFields(placement, closPlacements[closID])

    candidate := remaining
    if placement.allowedBitUsages not empty {
        candidate &= unionBitUsageMasks(placement.allowedBitUsages)
    }

    expr := resolvedExpressionFor(closID)
    ways := expr.Evaluate(capability)
    mask := contiguousMaskWithDirection(candidate, ways, placement.direction)
    if mask == 0 {
        return error
    }
    targets[closID][domain] = mask
    remaining &^= mask
}
```

Because the group order is explicit and `remaining` is derived from the same initial `available` mask every time, repeated reconciliations do not drift. A manual schemata edit does not influence the next desired target; the next reconcile rewrites the deterministic target.

If a group references a CLOS that is not currently managed, the implementation should fail closed rather than silently ignore it. AQC that names the wrong CLOS should not appear successful while leaving the intended CLOS unconstrained.

## Non-group CLOS behavior

Managed CLOS IDs that are not in an allocation group are processed independently. They use their resolved expression and resolved placement policy. Independent CLOS masks may overlap unless they are placed in an allocation group. This preserves existing configuration behavior without maintaining a separate compatibility code path.

If a non-group CLOS uses `allowedBitUsages`, it still avoids forbidden regions such as `X`, but it does not reserve those bits against other independent CLOS IDs.

## Mask validation

All target generation paths must share the same validation:

- `CBMMask` must be non-zero and contiguous.
- Evaluated ways must be positive.
- Evaluated ways must be at least the raw capability field
  `capability.MinCBMBits`.
- Evaluated ways must not exceed the number of available bits after placement filtering.
- A contiguous mask of the requested width must exist in the available mask.
- The chosen mask must be a subset of `capability.CBMMask`.
- If placement uses `allowedBitUsages`, the chosen mask must be a subset of the union of those usage masks.

Errors are wrapped at each boundary. Group allocation errors include the group, CLOS ID, and domain; expression evaluation errors also include the canonical expression:

```text
cat allocation group "gpu-safe-shared" cannot allocate CLOS "share-02" on domain 0: domain 0 cannot satisfy 9 CAT ways
```

## Rollback

Rollback keeps the existing safety behavior but uses expressions:

- If `defaultCATWays` is configured, evaluate it with the same capability and placement-independent full `CBMMask` target generation.
- If `defaultCATWays` is not configured, restore full `CBMMask`.
- Rollback should not use allocation groups, because it is a safety reset path rather than a policy packing path.

If the default expression itself cannot satisfy hardware capabilities, rollback returns an aggregate error with the original apply error, matching the current pattern around `catRollbackTarget`.

## API validation

CRD validation should remain conservative:

- `enableCAT=true` requires `defaultCATWays`.
- Integer `defaultCATWays` and `closCATWays` values must be greater than zero; these positive integer literals are the compatibility surface.
- String values must be non-empty.
- `catPolicy.defaultPlacement.direction` and group direction must be enum values.
- `allowedBitUsages` must be enum values.
- CLOS map keys and allocation group members must not be empty or contain whitespace.
- `allocationGroups[].closIDs` must contain at least one unique member.
- Expressions that are statically non-positive or contain literal-only arithmetic must be rejected.
- Expressions containing the legacy `CBMMask` or `MinCBMBits` operands must be rejected rather than translated.

The API uses CEL to reject malformed or statically invalid values before persistence. The core parser remains the runtime source of truth and repeats syntax and static-result validation for CLI input and defensive handling. API schema tests verify the int-or-string shape, CEL rules, exact enum sets, required group members, and removal of unsupported fields.

## Test plan

API tests should cover JSON round trip for both integer and string forms:

```json
{"defaultCATWays":4}
{"defaultCATWays":"MaxCATWays-MinCATWays"}
```

CRD schema tests should cover:

- `defaultCATWays` is `x-kubernetes-int-or-string`.
- `closCATWays` map values are int-or-string.
- `catPolicy.defaultPlacement.allowedBitUsages` has enum values.
- `catPolicy.allocationGroups[].direction` has enum values.
- `enableCAT=true` still requires `defaultCATWays`.

Core parser tests cover literals, `MaxCATWays`, `MinCATWays`, binary addition/subtraction, operator-adjacent whitespace normalization, operand-internal whitespace rejection, unknown tokens, zero and negative literals, literal-only arithmetic, statically non-positive expressions, chained expression rejection, and explicit rejection of legacy operands.

Capability provider tests should cover:

- `bit_usage` parsing for multiple domains.
- missing `bit_usage` tolerated when no placement policy requires it.
- malformed domain or usage strings return descriptive errors.
- bit order mapping fixed by tests using a known mask.

CAT plugin tests should cover:

- existing integer behavior unchanged.
- `MaxCATWays`, `MinCATWays`, `MaxCATWays-MinCATWays`, and `MaxCATWays-2` expressions over domains with different raw `CBMMask` widths.
- placement with `allowedBitUsages: ["S"]` excludes `X`.
- `direction: low` and `direction: high` choose stable opposite masks.
- allocation group packs in explicit `closIDs` order and produces non-overlapping masks.
- repeated reconcile writes the same targets in the same order.
- CLOS list order and domain map order do not change targets.
- allocation group fails closed when remaining `S` bits cannot satisfy a later CLOS.
- invalid group references and duplicate group membership fail before writes.

## Worktree and branch layout

The implementation should stay isolated from dirty main workspaces:

| Repository | Base branch | Worktree | Feature branch |
|---|---|---|---|
| core | `feat/default-share-residual-backfill` | `katalyst-core/.worktrees/default-share-cat-ways-expression` | `feat/default-share-cat-ways-expression` |
| API | `feat/default-share-residual-backfill-api` | `katalyst-api-cat-ways-expression` | `feat/default-share-cat-ways-expression-api` |
| adapter | `feat/default-share-residual-backfill-adapter` | optional follow-up | `feat/default-share-cat-ways-expression-adapter` |

Adapter changes are expected to be small because the existing build script already passes `QRMCPUPluginBulkheadDefaultCATWays` and `QRMCPUPluginBulkheadClosCATWays` to the unchanged flag names. Structured `catPolicy` should be configured through AQC, not environment variables.

## Implementation order

The safest implementation order is:

1. Update API types to `IntOrString` and add `CATPolicy`.
2. Regenerate deepcopy and CRD, then update API tests.
3. Add core `CATWaysExpression` parser and evaluator with isolated tests.
4. Extend `CATCapabilityProvider` to parse optional `bit_usage`.
5. Add core placement and allocation group types.
6. Convert CLI options from integer boundary to string boundary and parse in `ApplyTo`.
7. Convert AQC `IntOrString` fields into core expressions in dynamic config apply.
8. Refactor CAT plugin target building into deterministic dry-run functions.
9. Add allocation group packing and stable target/apply sorting.
10. Run focused API and core unit tests before any node validation.

## Local verification

Focused API verification:

```bash
go test ./pkg/apis/config/v1alpha1
```

Focused core verification:

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm ./pkg/config/agent/dynamic/adminqos/qrm ./pkg/util/external/rdt ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat
```

Node validation should use AQC to exercise:

- `defaultCATWays: MaxCATWays`
- `closCATWays` values `MaxCATWays-MinCATWays`, `MinCATWays`, and `MaxCATWays-2`
- `allocationGroups` restricted to `allowedBitUsages: ["S"]`
- repeated AQC reconcile with identical target masks
- an intentionally oversized group that fails without partial CAT writes

## Open implementation notes

The only kernel detail that must be verified during implementation is the exact mapping from `bit_usage` string position to numeric CBM bit. The design requires deterministic numeric masks, so this mapping must be captured in unit tests before the allocation algorithm is connected to the CAT plugin.

The first implementation should keep automatic packing limited to `allocationGroups`. Independent CLOS placement should remain independent and potentially overlapping, matching existing behavior unless the user explicitly opts into group packing.
