# Effective Pod Request Resources Design

## Goal

Provide one native helper that calculates Kubernetes Pod effective requests correctly when Init Containers and Pod overhead are present, and make the existing request-summing helper use that behavior.

## API

Add the following function in `pkg/util/native/pods.go`:

```go
func EffectivePodRequestResources(pod *v1.Pod) v1.ResourceList
```

`SumUpPodRequestResources` remains exported with its existing signature and delegates to `EffectivePodRequestResources`. Existing callers do not need source changes.

## Resource Semantics

For every resource name, the helper returns:

```text
max(sum(app container requests), max(init container requests)) + pod overhead
```

The app-container total and Init Container maximum are computed independently. Pod overhead is applied only after the maximum is selected, so a larger Init Container request cannot discard overhead already required by the Pod.

Resource quantities are compared with `resource.Quantity.Cmp`, preserving milli-scale CPU and extended-resource precision.

## Compatibility

The helper intentionally keeps a no-error return signature. Existing core call sites expect a resource list and Kubernetes Pod API objects are trusted on normal core paths. Adapter inventory admission remains responsible for its additional nil-input, negative-quantity, and overflow validation; this native helper does not absorb those resource-plugin boundary checks.

## Tests

`pkg/util/native` tests must cover:

- an Init Container request larger than all app containers while retaining Pod overhead;
- an app-container total larger than Init Container requests;
- milli-scale quantity comparison;
- equality between `SumUpPodRequestResources` and `EffectivePodRequestResources`.

No existing call site changes are required. Targeted native tests and the impacted resource-accounting package tests must pass.
