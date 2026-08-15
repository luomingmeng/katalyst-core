# CAT zero ways design

## Background

The RDT CAT bulkhead plugin treats `min_cbm_bits=0` as a valid hardware
capability after commit `c166192d5`. AMD resctrl may expose that value because
the hardware accepts an empty CAT capacity bitmask. The expression parser
already accepts symbolic `MinCATWays`, but runtime evaluation and target
construction currently reject its result when it evaluates to zero.

On the validation node, the L3 CAT capability is:

```text
cbm_mask=ffff
min_cbm_bits=0
```

With `closCATWays.sandbox=MinCATWays`, the desired hardware target is therefore
an empty L3 mask for every domain.

## Goals

- Allow a valid symbolic CAT ways expression to evaluate to zero when the
  domain capability reports `MinCBMBits == 0`.
- Build and apply an empty L3 CAT mask for that CLOS and domain.
- Preserve the existing API and CLI contract that explicit integer or string
  literal zero is invalid.
- Preserve fail-closed behavior on hardware whose minimum CAT width is
  positive.
- Preserve deterministic, all-targets-before-writes reconciliation.

## Non-goals

- Do not add zero as an API or CLI literal.
- Do not reinterpret zero as one.
- Do not enable an empty CAT mask on Intel or any capability with
  `MinCBMBits > 0`.
- Do not change CAT placement, CLOS key resolution, or AQC precedence.
- Do not introduce an alternate expression variable for a positive minimum.

## Configuration contract

The expression grammar remains unchanged:

```text
operand := positive_integer | "MaxCATWays" | "MinCATWays"
```

Explicit zero remains invalid:

```yaml
defaultCATWays: 0
closCATWays:
  sandbox: 0
```

Statically zero expressions remain invalid:

```text
MaxCATWays-MaxCATWays
MinCATWays-MinCATWays
```

The only new behavior is that an otherwise valid symbolic expression may
produce zero from runtime capability inputs. The intended configuration is:

```yaml
defaultCATWays: MaxCATWays-MinCATWays
closCATWays:
  sandbox: MinCATWays
```

On a domain with `MaxCATWays=16` and `MinCATWays=0`, the resolved targets are:

```text
default = 16
sandbox = 0
```

## Runtime behavior

`CATWaysExpression.Evaluate` must reject negative results but return zero
without error. Parser validation remains responsible for rejecting explicit
and statically zero configurations.

`targetForAvailable` must handle zero before contiguous-mask construction:

1. Evaluate the expression.
2. Reject a negative result.
3. If the result is zero, require `capability.MinCBMBits == 0` and return mask
   `0`.
4. For a positive result, keep all existing minimum, capacity, placement, and
   contiguity validation.

The zero path must not call `contiguousMask`, because an empty mask is a valid
target rather than an allocation failure.

## Allocation semantics

An exclusive CLOS whose target is zero consumes no bits from the per-domain
remaining mask:

```text
target = 0
remaining &^= target
remaining is unchanged
```

Other CLOS IDs may therefore consume the entire remaining mask. The masks are
still non-overlapping because `0 & peerMask == 0`.

All CLOS and domain targets must still be built before the first schemata
write. An invalid positive-width allocation or unsupported zero target must
fail without partial writes.

## Hardware safety

The capability gate is the resctrl ABI value itself. A zero target is allowed
only when `MinCBMBits == 0`. A capability with a positive minimum continues to
reject a zero runtime result before any write.

Applying a zero mask does not stop CPU execution, but it prevents the CLOS from
allocating new LLC lines. L1, L2, and memory access continue to operate. This
can cause severe workload performance degradation, so zero remains available
only through a hardware-derived symbolic expression rather than an explicit
literal.

## API impact

No API or CRD change is required. `defaultCATWays` and `closCATWays` keep their
current `IntOrString` schema and positive-integer validation. Existing CEL
rules that reject explicit or statically non-positive expressions remain
unchanged.

No adapter mapping change is required. Existing environment variables remain
string pass-through values:

```bash
QRMCPUPluginBulkheadDefaultCATWays=MaxCATWays-MinCATWays
QRMCPUPluginBulkheadClosCATWays=sandbox=MinCATWays
```

## Error handling

- Negative runtime results remain errors.
- A zero runtime result on `MinCBMBits > 0` is an unsupported target error.
- Positive results below a positive `MinCBMBits` remain errors.
- Positive results that cannot fit the available mask remain errors.
- Apply failures retain the existing rollback behavior.

Errors must include the expression, CLOS ID, and domain through existing
wrapping layers.

## Test plan

Expression tests must verify:

- `MinCATWays` evaluates to zero when `minCATWays=0`.
- negative runtime results remain rejected.
- explicit literal `0` remains rejected.
- statically zero expressions remain rejected.

CAT target tests must verify:

- `MinCATWays` produces mask `0` when `MinCBMBits=0`.
- a positive expression keeps existing contiguous low/high placement.
- a zero runtime target is rejected when `MinCBMBits>0`.
- an exclusive zero target leaves the remaining mask unchanged.
- a zero target and a full-width peer target are non-overlapping.
- multi-domain target construction produces zero for every zero-minimum
  domain.
- an error in another target occurs before any schemata write.

Focused verification:

```bash
go test ./pkg/config/agent/dynamic/adminqos/qrm
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat
go test ./pkg/util/external/rdt
```

## Node validation

Build the agent from `feat/default-share-residual-backfill`, upload it to
`fdbd:dc05:d:44e::17`, and preserve the previous binary, startup script, and
schemata before replacement.

Use:

```bash
QRMCPUPluginBulkheadDefaultCATWays=MaxCATWays-MinCATWays
QRMCPUPluginBulkheadClosCATWays=sandbox=MinCATWays
```

After a controlled agent restart, verify:

- the process command line contains both CAT flags;
- the process stays running;
- healthz returns HTTP 200 across at least three periodical cycles;
- no `rdt_cat` periodical failure is logged;
- every sandbox L3 domain reads back mask `0`;
- the root/default schemata remains `ffff`;
- repeated reconciliation preserves the same masks;
- no unexpected CLOS or workload is attached to the zero-mask sandbox during
  validation.

Keep the validated binary in place. Restore the original startup configuration
and schemata after validation unless the operator explicitly requests the test
policy to remain active.
