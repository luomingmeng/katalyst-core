---
title: Generic Control-Knob Restrict Rules
authors:
  - "@luomingmeng"
reviewers:
  - "@qos-sysadvisor"
creation-date: 2026-10-29
last-updated: 2026-10-30
status: implemented
---

# Generic Control-Knob Restrict Rules

## Summary

The sysadvisor CPU region already clamped one policy's delivered control knob
against another policy's same-period knob (the pre-existing exact policy-reference
restrict path, a strict ref-only cap on the policy-level target->reference map).
This design extracts the non-reclaimed-cpu upper-bound safety-valve into a small,
knob-agnostic bounded constraint engine: an ordered list of declarative one-sided
upper bounds on `(targetPolicy, referencePolicy, controlKnob)`, composed
deterministically and driven entirely by dynamic AQC. The engine contains no
Rama-specific code and no knob-level policy map.

## Motivation

The first safety-valve was a dedicated `restrictRAMANonReclaimedCPURequirement`
path plus a Rama-specific metric. It hardcoded target=Rama, reference=canonical,
and the knob name in the executor, making it impossible to restrain other
policy/knob combinations without new code and a new metric. We need a reusable,
observable constraint layer that:

- supports multiple target policies, distinct knobs, and a selectable
  reference policy per rule;
- composes multiple rules on the same `(target, knob)` with a defined result;
- fails open (never drives a knob below an absent reference);
- hot-reloads from AQC with no stale residue;
- uses a single external config entry instead of a parallel rule list.

## Design

### Startup seed flag and tri-state AQC override

There is a **single** dynamic field,
`CPUProvisionConfiguration.ControlKnobConstraints` (typed
`[]ControlKnobConstraints`). There is no separate defaults field, no
`SetControlKnobConstraintsDefaults` method, and no restore path inside
`ApplyConfiguration`.

1. **Startup seed** — `--region-bounded-control-knob-constraints-json`, a
   strict JSON array decoded with `encoding/json.Decoder` +
   `DisallowUnknownFields` and a second `Decode` that must return `io.EOF`
   (trailing data rejected). Each startup entry must have a non-empty `name`,
   **both** `targetPolicy` and `referencePolicy` set (a name-only or half-explicit
   entry fails startup), at least one strictly positive tolerance, and no negative
   tolerance. The parsed slice is assigned directly to the dynamic
   `CPUProvisionConfiguration.ControlKnobConstraints` by
   `CPUProvisionOptions.ApplyTo`. `json.Decoder` produces a fresh object graph,
   so the flag-owned slice is not aliased.
2. **Dynamic AQC override** — `CPUProvisionConfig.Constraints` is
   `*[]ControlKnobConstraints`, a tri-state pointer consumed by
   `CPUProvisionConfiguration.ApplyConfiguration`:
   - `nil` (field omitted, or the whole CPUProvisionConfig absent/deleted):
     `ApplyConfiguration` leaves the current object's slice **untouched**.
   - `&[]` ("constraints: []"): the current slice is **cleared** (explicit empty
     override, not a fallback to defaults).
   - `&[entries]`: the current slice is **whole-replaced** by a deep copy of the
     AQC slice. It is never merged with the current value.

**Why there is no restore-to-defaults logic in `ApplyConfiguration`.** The KCC
dynamic config manager (`pkg/metaserver/kcc/manager.go`) already owns the
per-round lifecycle: at construction it deep-copies the flag-parsed
`dynamic.Configuration` once into `defaultConfig`; on every informer tick it
does `currentConfig := deepCopy(c.defaultConfig)` and then calls
`config.ApplyConfiguration(dynamicConfigCRD)` on the fresh object. So a new
object always re-enters `Apply` with the startup value A already in place.
`ApplyConfiguration` itself only needs to react to the incoming AQC pointer;
it does not promise to restore defaults if the *same* object is applied twice.
This removes the internal duplicate slice and the alias risk that came with
resetting it on every tick.

### Constraint-shape routing

The effective `ControlKnobConstraints` list drives both paths; routing is by the
shape of each entry (which of `targetPolicy` / `referencePolicy` are set):

| Shape | Route | Behavior |
| --- | --- | --- |
| both empty (`shapeNameOnly`) | runtime silent | double-empty entries are the compatibility form for the static exact path; the bounded executor skips them every tick with no metric and no warning. On config load, `ApplyConfiguration` emits **at most one** warning per application (not per entry, not per tick) if any entry is name-only |
| both set (explicit pair) | `applyBoundedControlKnobConstraints` | bounded upper/lower bound; explicit pair is the whole identity |
| exactly one set | `invalid-config` | fail-open, one warning, one `invalid-config` upper metric, no clamp |

`shapeNameOnly` is the only name-only shape: neither `targetPolicy` nor
`referencePolicy` is set. It is deliberately runtime-silent (no per-tick metric or
log) so existing v1 AQC entries keep working; the single config-load warning is
the only signal for an ambiguous double-empty entry.

`invalid-config` has exactly two causes, both fail-open: (a) a half-explicit entry
(exactly one of target/ref set), and (b) a fully-explicit bounded rule on a knob
owned by the exact path. There is no "single-rule lower>upper" invalid case: within
one rule lower and upper cannot invert (`lower <= ref < upper` for `ref > 0`), so
the evaluator contains no such branch.

The exact path uses a closed static knob set
(`ctrlKnobsNeedPolicyExactRestrict`, initialized to
`{reclaimed-cores-cpu-quota: true}`). It is completely independent of the dynamic
AQC `Constraints` slice.

### Exact policy-reference restrict path

`restrictControlKnobByExactPolicy` takes only `originControlKnob`,
`r.conf.ExactRestrictRefPolicy`, and the static `ctrlKnobsNeedPolicyExactRestrict`
set as inputs. It never reads the dynamic `Constraints` slice. For each target
policy it looks up the single `ExactRestrictRefPolicy[target]` reference; if the
reference produces the knob, it applies the exact `raw > ref -> ref` cap.

`CPURegionConfiguration.ExactRestrictRefPolicy` is
`map[CPUProvisionPolicyName]CPUProvisionPolicyName` (one-to-one, policy-level,
no knob dimension). The flag is `--region-exact-restrict-ref-policy`
(`target=ref`, e.g. `rama=dynamic-quota`); the old flag name has no alias.
Default remains `rama -> dynamic-quota`, so existing behavior is unchanged.

### Bounded control-knob constraint path

`applyBoundedControlKnobConstraints` requires both `targetPolicy` and
`referencePolicy` to be explicit. It reads `ExactRestrictRefPolicy` never; it
reads `ctrlKnobsNeedPolicyExactRestrict` only for ownership validation: a fully
explicit bounded constraint whose `Name` is in that static set is treated as
invalid-config fail-open (the knob is owned by the exact path). It iterates the
AQC slice, skipping name-only entries silently. An exact explicit
`(target, ref, knob)` duplicate replaces the earlier entry (last-wins in
reverse scan); surviving entries keep declaration order.

The relevant fields are those on `ControlKnobConstraints`:

| Field | Meaning |
| --- | --- |
| `TargetPolicy` | policy whose delivered knob may be clamped (explicit) |
| `ReferencePolicy` | same-period baseline (explicit) |
| `Name` | knob key shared by both policies |
| `MaxUpperGap` (ptr) | absolute upward tolerance; enabled only when `> 0` |
| `MaxUpperGapRatio` (ptr) | relative upward tolerance of the reference; enabled only when `> 0` |
| `MaxLowerGap` (ptr) | absolute downward tolerance; enabled only when `> 0` |
| `MaxLowerGapRatio` (ptr) | relative downward tolerance of the reference; enabled only when `> 0` |
| `Enforce` (ptr) | `true` rewrites on overshoot; otherwise shadows/observes only |

Upper bound: `upper = ref + min(enabled MaxUpperGap, enabled MaxUpperGapRatio * ref)`.
Lower bound: `lower = max(0, ref - min(enabled MaxLowerGap, enabled MaxLowerGapRatio * ref))`.
Each side is enabled independently only when its gap/ratio is strictly positive.
Within a single rule the two sides cannot invert (`lower <= ref < upper` for
`ref > 0`), so there is no single-rule lower>upper branch in the evaluator.

- The executor reads each rule's raw target and reference from the **original**
  knob map (no feed-forward); clamps apply once after all rules are evaluated.
- Different references for the same `(target, knob)` coexist in order.
- **Aggregation is over all enabled sides, never gated on overshoot.** Every
  successful enforce rule contributes each of its enabled sides (`upperOn` and/or
  `lowerOn`) to the per-`(target, knob)` aggregate, regardless of whether raw
  currently overshoots that side. `upperOver`/`lowerOver` only select the per-side
  metric action (`passthrough` vs `observed`/`clamped`); they never decide whether
  a bound enters the aggregate. Enforce upper rules compose by `minUpper`; enforce
  lower rules compose by `maxLower`.
- **Conflict is strict.** A conflict is `maxLower > minUpper` (strict). On
  conflict the `(target, knob)` fails open (raw stands unchanged) and emits exactly
  one `conflicting-bounds` upper-metric observation. Equality is **not** a
  conflict: when `maxLower == minUpper` delivery clamps to the shared bound and no
  `conflicting-bounds` observation is emitted.
- Adversarial inversion: an upper rule anchored on `ref=30` (upper bound 40) and a
  lower rule anchored on `ref=120` (lower bound 60) invert across references
  (`60 > 40`). This must surface as a conflict whether the target raw sits below
  both (raw=30) or above both (raw=70): in both cases delivery stands at raw and
  exactly one `conflicting-bounds` observation is emitted. If aggregation were
  overshoot-gated, neither side would contribute in these two positions and the
  conflict would be silently lost.
- **Cloning is one deep clone per target policy.** A target policy's `ControlKnob`
  map is cloned at most once (the first time one of its knobs is clamped, via a
  per-target cache); all of that policy's clamped knobs then write into the same
  clone. The input inner maps are never mutated in place, and the original knob map
  passed in is never aliased by a clamp.
- Two gauges: `cpu_provision_control_knob_restrict_upper_bound` and
  `cpu_provision_control_knob_restrict_lower_bound`, same tag set. Enabled sides
  emit their own gauge; error/fail-open paths emit a single upper gauge with the
  raw value. There is no direction tag.

### Failure semantics

The executor is knob-/policy-agnostic; failure is fail-open and isolated:

- Target policy missing from the control-knob map, **or** the target policy is
  present but the rule's knob is absent from its map: `target-unavailable`, raw
  stands. There is no longer an `unknown-policy` action; the configured target
  string appears only in the warning, and the `target_policy` / `control_knob`
  tags are normalized to `unknown` for label stability.
- Reference policy/knob missing, unresolved, or reference NaN/Inf/`<= 0`:
  `ref-unavailable`, raw stands.
- Constraint present but no positive tolerance: `no-positive-tolerance`, raw
  stands.
- A fully-explicit bounded constraint on a knob owned by the exact path (in
  `ctrlKnobsNeedPolicyExactRestrict`) is `invalid-config` fail-open; this is the
  only bounded read of the exact set and does not alter exact-path execution.
- `maxLower > minUpper` across composed references: `conflicting-bounds`, raw
  stands.

The bounded path intentionally has no requests-floor invariant: an upper or
lower candidate may sit below the region's total requests; the bound is applied
purely relative to the reference policy's value.

### Wiring and scope

Both `QoSRegionShare.TryUpdateProvision` and `QoSRegionDedicated.TryUpdateProvision`
run the same order: update policies → raw control knobs →
`restrictControlKnobByExactPolicy` → `applyBoundedControlKnobConstraints` →
regulation. The bounded pass reads the exact-path output, so the order is
preserved. Isolation and other exclusive region types do **not** call the
bounded pass (they have their own `TryUpdateProvision` and were not wired).
The same AQC `Constraints` apply to share and dedicated when their target/ref/
knob outputs exist; metrics carry `region_type` and `region_name` so same-type
regions on one node do not alias. Because the KCC manager rebuilds `currentConfig`
from `defaultConfig` on every tick, hot-delete of the AQC `Constraints` pointer
takes effect by the next round (the fresh object re-enters `Apply` with the
startup seed A, and a nil AQC pointer leaves A untouched); `constraints: []`
clears the slice on that object; a non-empty AQC slice whole-replaces it. There
is never a merge between the startup seed and the AQC slice, and no informer-object
aliasing because `ApplyConfiguration` deep-copies the incoming AQC slice.

### Metric contract

Two gauges: `cpu_provision_control_knob_restrict_upper_bound` and
`cpu_provision_control_knob_restrict_lower_bound`. Tags are low-cardinality
only. The fixed seven keys, identical on both gauges:

| Key | Source |
| --- | --- |
| `region_type` | `QoSRegionBase.regionType` |
| `region_name` | `QoSRegionBase.name` (the region's stable UUID allocated at construction; required because multiple regions can share one `region_type` on a node and would otherwise alias) |
| `target_policy` | target policy name, normalized to `unknown` when the policy is absent from the control-knob map |
| `reference_policy` | reference policy name, normalized to `unknown` when absent |
| `control_knob` | knob name, normalized to `unknown` when the target policy or its raw knob is absent |
| `mode` | `shadow` or `enforce` |
| `action` | per-side outcome, see below |

When a rule has the upper band on, the upper gauge is emitted with the upper
bound as value; when the lower band is on, the lower gauge is emitted with the
lower bound as value. A rule with both bands on emits **both** gauges, each
with its own per-side action. On fail-open paths (no bands, ref-unavailable,
no-positive-tolerance, invalid-config, target-unavailable, conflicting-bounds)
exactly one upper gauge is emitted with the raw value (so it never reads `0`,
a falsely tight bound). Unregistered target/reference policies are labeled
`unknown`; the configured string appears only in the warning. The
`control_knob` tag follows the same rule: it carries the real knob name only
when the target policy **and** its raw knob both exist in the control-knob map;
otherwise it is `unknown`, so a typo target or an absent knob never leaks a
configured knob string into the label set.

`action` values (the five error values imply fail-open): `passthrough`
(within band), `observed` (shadow overshoot), `clamped` (enforce overshoot),
`ref-unavailable`, `no-positive-tolerance`, `invalid-config` (a half-explicit
entry, or a bounded rule on an exact-owned knob), `conflicting-bounds`
(composed `maxLower > minUpper`), and `target-unavailable` (target policy
missing from the control-knob map, or the target policy is present but the
rule's knob is absent from its map). The historical `unknown-policy` value is
**removed**; callers that filtered on it should switch to
`action=target-unavailable` (and the `target_policy=unknown` /
`control_knob=unknown` label pair, which is unchanged). `mode` is `shadow` or
`enforce`.

**Old → new monitoring query migration (breaking, no dual-write).** The tag
set gains exactly one key, `region_name`; the other six keys are unchanged
from the pre-this-commit bounded emit. The action enum loses `unknown-policy`
and gains `target-unavailable`. Dashboards/alerts that aggregated by
`unknown-policy` must be updated to `target-unavailable`; queries that did not
select `unknown-policy` need no change except adding `region_name` if they want
to split same-type regions. There is no side-by-side emission of old and new
tags.

## API dependency

The engine consumes `CPUProvisionConfig.Constraints`, now typed as
`*[]ControlKnobConstraints` (the tri-state pointer), with entries carrying
`name`, the inline `RestrictConstraints` gaps, the optional `enforce` flag, and
the optional `targetPolicy` / `referencePolicy` strings. The API feature commit is
`b89e78e042a6fc65b4ac759587872cec0b3e4df6` layered on base `4f4bdd7`; it
retains `CPUProvisionPolicyName` and its `canonical`/`rama` constants, the
generated deepcopy (including the `enforce` pointer), and the CRD schema.

## Release blocker (must resolve before merge)

The Core worktree pins the API through a **personal fork** in `go.mod`:

```
replace github.com/kubewharf/katalyst-api => github.com/luomingmeng/katalyst-api v0.0.0-20261030041238-b89e78e042a6
```

This replace (and its `go.sum` entries) must be removed before merge and replaced
by a published upstream `kubewharf/katalyst-api` pseudo-version carrying the
`b89e78e` API commit. Core is otherwise one clean commit over `3a53b9d47`;
`gofmt`, `go vet`, and the region/config/options suites pass.

## Test matrix

- `evaluateBounds` pure cases: ref NaN/Inf/zero/negative, no positive tolerance,
  within band, overshoot by gap/ratio (upper side exercised directly).
- Executor table: disabled passthrough, enforce gap clamps, shadow observes,
  within-band passthrough, ref/target absent, unknown target/ref no-panic, no
  positive tolerance; upper clamps even when the candidate is below region
  requests.
- Metric labels: target/reference/`control_knob` are `unknown` for a typo target
  and for a known target whose raw knob is absent; the configured knob string
  never leaks; a known target with the raw knob present keeps the real knob label.
- Decoupling: omitted target and omitted reference each fail-open as
  `invalid-config`; changing/emptying `ExactRestrictRefPolicy` has zero effect on
  an explicit bounded rule result; exact-path quota still follows
  `ExactRestrictRefPolicy`.
- Composition: two enforce rules take the min upper; a tighter shadow does not
  lower delivery; two references on the same target/knob coexist; explicit
  composite duplicate last-wins.
- Adversarial inversion: upper=40 (ref 30) and lower=60 (ref 120) across
  references conflict for raw=30 and for raw=70 in turn; delivery stays at raw and
  exactly one `conflicting-bounds` observation is emitted in both positions.
- Equality boundary: upper=15 and lower=15 meet exactly; delivery clamps to 15
  and no `conflicting-bounds` observation is emitted (strict-`>` conflict).
- Exact-set membership: name-only quota is clamped; a name-only knob outside the
  static set is ignored with no clamp; explicit bounded entry on a static-set
  knob is invalid-config fail-open (does not alter exact-path execution).
- Exact-path independence: exact result is identical for
  `DynamicConfiguration == nil`, empty/arbitrary name-only constraints, and
  arbitrary explicit pairs; only `ExactRestrictRefPolicy` and the static set
  change the exact result.
- Config (semantic-free): each test seeds a startup A into a dynamic
  `CPUProvisionConfiguration`, then on a fresh `syntax.DeepCopy(seed)` object
  (mirroring the KCC manager's per-tick rebuild) applies the AQC override.
  A nil AQC `Constraints` pointer leaves A untouched, `&[]` clears, and
  `&[entries]` whole-replaces with no merge; deep-copy does not alias
  informer- or flag-owned pointer fields; several name-only AQC entries trigger
  exactly one warning per `ApplyConfiguration` (the startup flag itself rejects
  name-only entries). Deleting the AQC `Constraints` pointer in the next round
  restores A because the KCC manager deep-copies the startup-seeded
  `defaultConfig` again — not because `ApplyConfiguration` itself restores
  defaults on the same object.
- Startup flag: `--region-bounded-control-knob-constraints-json` end-to-end via
  pflag; malformed JSON, trailing data (second `Decode` not `io.EOF`), unknown
  keys, and a non-array document all fail startup; empty name, name-only,
  half-explicit target/ref, negative tolerance, and zero/absent positive tolerance
  all fail startup.

### Ownership and upgrade order

Ownership of the exact path is **not** re-declared in Core: the bounded executor
reuses the existing static `ctrlKnobsNeedPolicyExactRestrict` set at runtime and
fails open on a bounded entry that targets an exact-owned knob. No copy of the
owned-knob list is maintained anywhere else.

Rollout order: (1) ship the binary with the new flag, optionally providing bounded
seed defaults; (2) AQC `Constraints` then overrides tri-state. To disable bounds
at runtime without redeploying, set AQC `constraints: []` (clears on the next
round's fresh object); to return to the flag seed, omit/delete the field — the
KCC manager rebuilds `currentConfig` from `defaultConfig` on the next tick, so
the fresh object re-enters `Apply` with the startup seed A intact. Rollback is
safe: a binary without this feature ignores the unknown AQC field and the startup
flag, and existing name-only AQC entries keep the prior exact-path behavior.
- Wiring regression: share runs exact-path then bounded pass; isolation is not
  wired. Dedicated behavior is covered end-to-end through the real
  `QoSRegionDedicated.TryUpdateProvision` (exact-then-bounded order, exact-owned
  invalid-config rejection, shadow/enforce upper and lower bands, half-explicit
  invalid-config) rather than a duplicate helper-only test; exact-path quota
  `rama -> dynamic-quota` output is unchanged.

## Non-goals

- A requests-floor invariant on bounded clamps: an upper/lower candidate may sit
  below the region's total requests.
- Feed-forward between rules or cross-rule state.
- A knob-level parallel policy map or a parallel rule list.
