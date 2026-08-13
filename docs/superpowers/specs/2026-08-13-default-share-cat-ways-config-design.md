# Default Share CAT Ways Startup Configuration

## Goal

Allow operators to configure bulkhead RDT `DefaultCATWays` and `ClosCATWays`
through katalyst-agent command-line flags and katalyst-adapter environment
variables, while preserving AdminQoSConfiguration as the dynamic override.

## Baselines

- katalyst-core: `feat/default-share-residual-backfill` at `34b597d2d`
- katalyst-adapter: `feat/default-share-residual-backfill-adapter` at `ef0b5389`
- katalyst-api already defines `DefaultCATWays` and `ClosCATWays`; no API change
  is required.

## Interfaces

katalyst-core adds the following flags to the `qrm-cpu-plugin` flag set:

- `--bulkhead-default-cat-ways=<positive int64>`
- `--bulkhead-clos-cat-ways=<clos=positive-int64,...>`

katalyst-adapter maps the following environment variables:

- `QRMCPUPluginBulkheadDefaultCATWays` to `bulkhead-default-cat-ways`
- `QRMCPUPluginBulkheadClosCATWays` to `bulkhead-clos-cat-ways`

`bulkhead-clos-cat-ways` uses pflag's `StringToInt64` value with the same
Kubernetes key-value input format.
For example, `reclaim=2,shared=4` becomes
`map[string]int64{"reclaim": 2, "shared": 4}`.

## Core Design

`CPUPluginOptions` owns startup defaults. It registers `DefaultCATWays` with
pflag's native `Int64Var` and retains the registered `*pflag.Flag` so
`ApplyTo` can inspect `Flag.Changed`. This distinguishes an omitted flag's
compatible zero value from an explicitly configured zero without a custom
`pflag.Value`.

`ClosCATWays` is stored directly as `map[string]int64` and registered with
`StringToInt64Var`. Numeric conversion and malformed value rejection happen
during flag parsing; `ApplyTo` only enforces domain validation and writes the
typed map into `DynamicBulkheadRDTConfiguration`.

Validation rejects:

- non-positive explicitly configured `DefaultCATWays`; an omitted flag keeps
  the compatible zero value;
- empty CLOS names;
- non-integer CLOS way counts during flag parsing;
- non-positive CLOS way counts.

The zero-value startup configuration remains valid and preserves existing
behavior when neither flag is supplied.

## Precedence

Startup flags populate the initial dynamic QRM configuration. Existing
AdminQoSConfiguration application remains unchanged: non-nil
`DefaultCATWays` and `ClosCATWays` fields replace the startup values.
SysAdvisor remains the owner of dynamic configuration.

## Adapter Design

The adapter only extends `param_map` in
`build/katalyst-agent/bytedance_run.sh`. It does not parse or validate CAT
values; core remains the single owner of type conversion and validation.
The adapter must also lock its katalyst-core replacement to the core commit
that registers these flags; publishing the core branch precedes adapter
dependency resolution and build verification.

## Testing

Core tests cover:

- both flags are registered;
- valid scalar and StringToInt64 values reach the typed configuration;
- malformed or non-integer CLOS values fail during flag parsing;
- malformed and empty-key values are rejected, and per-CLOS ways reject zero
  and negative values;
- a real parse of `--bulkhead-default-cat-ways=0` succeeds at flag parsing but
  is rejected by `ApplyTo` with a lower-case error;
- omitted flags preserve zero-value behavior.

Adapter verification covers:

- both environment variables map to the expected flag names;
- the locked core revision contains both mapped flags;
- the startup script passes `bash -n`.

## Scope

This change does not add `EnableCAT` startup configuration, modify API types,
change dynamic AQC semantics, or alter CAT allocation behavior.
