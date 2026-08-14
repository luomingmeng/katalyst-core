# CAT Ways Expression Naming Design

## Goal

Replace the hardware-oriented CAT expression operands `CBMMask` and
`MinCBMBits` with way-count operands that describe the values exposed to users:

```text
MaxCATWays
MinCATWays
```

This is a hard contract change. The old operands are invalid after the change
and are not retained as aliases.

## Semantics

`MaxCATWays` is the number of allocatable CAT ways on one cache domain. It is
derived from the population count of the domain's `cbm_mask`, not from the
numeric mask value. For a domain with `cbm_mask=0x7fff`, `MaxCATWays` is `15`.

`MinCATWays` is the hardware minimum number of contiguous CAT ways accepted by
the domain. It is derived from `min_cbm_bits`. For a domain with
`min_cbm_bits=1`, `MinCATWays` is `1`.

The expression:

```text
MaxCATWays-MinCATWays
```

therefore evaluates to `14` on that domain. Placement policy determines which
14 bits are selected; high placement over `0x7fff` produces `0x7ffe`.

## Expression contract

The supported grammar remains:

```text
term
term + term
term - term
```

Each term is one of:

```text
positive integer literal
MaxCATWays
MinCATWays
```

All existing whitespace, overflow, positivity, and symbolic-operand validation
rules remain unchanged.

The following inputs become invalid:

```text
CBMMask
MinCBMBits
CBMMask-MinCBMBits
```

No compatibility alias or automatic migration is provided because the feature
has not been released from the feature branches.

## Code changes

The API CEL grammar and generated CRD schema accept only the new names. API
schema tests must prove that the old names are rejected.

The core parser, operand enum names, string rendering, evaluator field names,
CLI defaults, unit tests, plugin tests, and error cases use the new vocabulary.
Internal RDT capability fields may retain `CBMMask` and `MinCBMBits` because
those names represent Linux resctrl files and raw hardware capability data, not
the user-facing expression language.

Adapter startup variables remain:

```text
QRMCPUPluginBulkheadDefaultCATWays
QRMCPUPluginBulkheadClosCATWays
```

Only their configured expression values change.

## Documentation changes

Update the CAT expression design, implementation plan, examples, test evidence,
and node validation report. Documents that discuss the raw Linux files
`cbm_mask` or `min_cbm_bits` keep those kernel names while mapping them to
`MaxCATWays` and `MinCATWays`.

## Verification

Required verification:

- API schema tests accept `MaxCATWays`, `MinCATWays`, and their binary forms.
- API schema tests reject every old operand and old binary form.
- Core parser and evaluator tests cover both new operands.
- CLI parsing tests cover new default and per-CLOS expressions.
- CAT plugin tests evaluate per-domain `MaxCATWays-MinCATWays`.
- Repository search finds no old operand in user-facing expression examples.
- Raw capability code may still contain `CBMMask` and `MinCBMBits`.
- Focused API/core tests, `go vet`, and `staticcheck` pass.

## Compatibility

This change intentionally breaks AQC or startup configuration that uses the old
operand names. Such configuration is rejected by API validation or core parsing
instead of being silently interpreted.
