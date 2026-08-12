# Dynamic Critical Watermark Source Design

## Context

Memory Guard currently selects `zoneInfo.Low` or `zoneInfo.High` through the
startup-only `--memory-advisor-critical-watermark-source` flag. The selected
value is stored in `MemoryAdvisorConfiguration.CriticalWatermarkSource`, while
the adjacent critical watermark scale factor already supports dynamic
AdminQoSConfiguration (AQC) updates.

Operators need to switch the watermark source through AQC without restarting
Katalyst Agent. The startup flag must remain backward compatible and serve as
the fallback when AQC does not specify the field.

## Goals

- Add `criticalWatermarkSource` to the AQC Memory Guard API.
- Support dynamic transitions between `low` and `high`.
- Preserve `--memory-advisor-critical-watermark-source` as the startup default.
- Restore the startup default when the AQC field is removed.
- Keep one runtime source of truth for the effective value.

## Non-Goals

- Changing critical watermark scale factor behavior.
- Changing `MinCriticalWatermark` behavior.
- Modifying QRM, CPU Advisor protocols, or default-share materialization.
- Adding adapter-specific interpretation of watermark source values.

## Repository Baselines

| Repository | Base branch | Base commit | Feature branch |
| --- | --- | --- | --- |
| Core | `feat/default-share-residual-backfill` | `34b597d2d` | `feat/critical-watermark-source-aqc` |
| Adapter | `feat/default-share-residual-backfill-adapter` | `ef0b5389` | `feat/critical-watermark-source-aqc-adapter` |
| API | `feat/default-share-residual-backfill-api` | `4c3f5b9` | `feat/critical-watermark-source-aqc-api` |

## Ownership Model

`dynamic.adminqos.advisor.MemoryGuardConfiguration` becomes the only runtime
owner of `CriticalWatermarkSource`.

The existing startup flag remains unchanged externally. Its value is applied
to the default dynamic Memory Guard configuration during startup. The KCC
manager already rebuilds each effective dynamic configuration from a deep copy
of the startup defaults before applying non-nil AQC fields. Therefore:

1. Without an AQC value, the effective value is the startup flag value.
2. With an AQC value, the AQC value overrides the startup default.
3. Removing the AQC value restores the startup default on the next update.

The static `MemoryAdvisorConfiguration.CriticalWatermarkSource` field is
removed after the flag wiring is migrated. Memory Guard must not implement a
second fallback or inspect both static and dynamic configurations.

## API Changes

Add the following optional field to
`config/v1alpha1.MemoryGuardConfig`:

```go
type CriticalWatermarkSource string

const (
	CriticalWatermarkSourceLow  CriticalWatermarkSource = "low"
	CriticalWatermarkSourceHigh CriticalWatermarkSource = "high"
)

// +kubebuilder:validation:Enum=low;high
// +optional
CriticalWatermarkSource *CriticalWatermarkSource `json:"criticalWatermarkSource,omitempty"`
```

The named type establishes a stable Go API contract and reusable values. The
pointer preserves the required three states:

- `nil`: use the startup default.
- `low`: explicitly select the low watermark.
- `high`: explicitly select the high watermark.

Regenerate or update the deepcopy implementation and
AdminQoSConfiguration CRD schema. The CRD enum is the primary admission
validation boundary.

## Core Changes

Extend `MemoryGuardConfiguration` with:

```go
CriticalWatermarkSource string
```

Its constructor default is `low`. Dynamic AQC application assigns the field
only when the API pointer is non-nil.

Move the existing flag field and validation into the dynamic Memory Guard
options path while preserving the public flag name
`memory-advisor-critical-watermark-source`. `ApplyTo` validates that the
startup value is `low` or `high` and writes it into the default dynamic Memory
Guard configuration. Validation errors remain lowercase.

`memory_guard.go` reads one dynamic configuration snapshot and uses its
`CriticalWatermarkSource` to select `zoneInfo.Low` or `zoneInfo.High`. The same
snapshot should also provide `CriticalWatermarkScaleFactor` so one calculation
does not observe fields from different dynamic revisions.

## Adapter Changes

The existing environment mapping remains valid:

```text
SysAdvisorMemoryAdvisorCriticalWatermarkSource
  -> memory-advisor-critical-watermark-source
```

No new adapter behavior is required. After API and Core commits are available,
update the Adapter API/Core dependency versions to the corresponding feature
commits. Do not commit local replace directives.

## Data Flow

```text
startup flag
  -> default dynamic MemoryGuardConfiguration
  -> KCC deep copy
  -> optional AQC MemoryGuardConfig override
  -> atomic SetDynamicConfiguration
  -> Memory Guard snapshot
  -> zoneInfo.Low or zoneInfo.High
  -> critical watermark scale factor
  -> minimum critical watermark clamp
  -> reclaimed memory limit
```

## Validation and Failure Behavior

- API admission rejects values outside `low` and `high`.
- Startup option validation rejects invalid flag values before agent startup.
- Dynamic configuration projection relies on the validated API contract. It
  does not widen the existing no-error `ApplyConfiguration` interface solely
  for this field.
- An absent AQC field is valid and restores the startup default.

## Testing

### API

- Deepcopy preserves a non-nil `criticalWatermarkSource`.
- CRD schema exposes the optional field with the `low` and `high` enum.

### Core options and configuration

- Constructor default is `low`.
- Startup flag `high` becomes the dynamic default.
- Invalid startup values return an error.
- AQC `high` overrides startup `low`.
- AQC `low` overrides startup `high`.
- Removing the AQC field restores the startup default.

### Memory Guard

- `low` selects `zoneInfo.Low`.
- `high` selects `zoneInfo.High`.
- Runtime updates affect the next calculation without restarting the agent.
- Source selection and scale factor come from one dynamic snapshot.

### Adapter

- Existing parameter mapping remains covered.
- Dependency metadata points to the final API and Core feature commits.

## Commit Order

1. API: add the optional AQC field, generated artifacts, and tests.
2. Core: migrate ownership and flag wiring, consume the dynamic field, and add
   tests.
3. Adapter: update API/Core dependency versions only.

Each repository change remains an independent atomic commit. The design commit
in Core is separate from implementation commits.

## Acceptance Criteria

- Updating AQC from `low` to `high`, or from `high` to `low`, changes Memory
  Guard behavior without restarting Katalyst Agent.
- Removing `criticalWatermarkSource` from AQC restores the startup flag value.
- The effective source has exactly one runtime owner.
- Existing deployments that only set the startup flag retain their behavior.
- Invalid API or startup values cannot become effective runtime configuration.
