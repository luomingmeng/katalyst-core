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

`bulkhead-clos-cat-ways` uses the Kubernetes `StringToString` input format.
For example, `reclaim=2,shared=4` becomes
`map[string]int64{"reclaim": 2, "shared": 4}`.

## Core Design

`CPUPluginOptions` owns startup defaults. It stores `DefaultCATWays` as
`int64` and receives `ClosCATWays` through a `map[string]string` flag value.
`ApplyTo` validates and converts every CLOS value to `int64`, then writes the
typed values into `DynamicBulkheadRDTConfiguration`.

Validation rejects:

- negative `DefaultCATWays`; zero means the startup value is not configured;
- empty CLOS names;
- non-integer CLOS way counts;
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

## Testing

Core tests cover:

- both flags are registered;
- valid scalar and StringToString values reach the typed configuration;
- malformed, empty-key, zero, and negative values are rejected;
- omitted flags preserve zero-value behavior.

Adapter verification covers:

- both environment variables map to the expected flag names;
- the startup script passes `bash -n`.

## Scope

This change does not add `EnableCAT` startup configuration, modify API types,
change dynamic AQC semantics, or alter CAT allocation behavior.
