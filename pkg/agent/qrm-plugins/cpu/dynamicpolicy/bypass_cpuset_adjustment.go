/*
Copyright 2026 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dynamicpolicy

import (
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	coreconsts "github.com/kubewharf/katalyst-core/pkg/consts"
)

// Strict annotation values for the public bypass-capability annotation. The
// annotation key itself is the shared coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey;
// these values are the only accepted strings.
const (
	bypassCapabilityTrue  = "true"
	bypassCapabilityFalse = "false"
)

// hasBypassableQoS reports whether the allocation's QoS level is eligible to skip
// response cpuset backfill at all. shared_cores / reclaimed_cores / system_cores
// are capable; dedicated_cores (and any other QoS) never are.
func hasBypassableQoS(info *state.AllocationInfo) bool {
	return info != nil &&
		(info.CheckShared() || info.CheckReclaimed() || info.CheckSystem())
}

// recordBypassCapability normalizes one main-container allocationInfo's public
// bypass-capability annotation at the allocation lifecycle. It is deterministic
// QoS labeling:
//   - dedicated (and any non-bypassable QoS) is always recorded as "false",
//     even if a user set "true";
//   - for shared/reclaimed/system, a valid explicit "true"/"false" is preserved
//     (an explicit "false" must NOT be overwritten with the QoS-derived "true");
//   - a missing key is written with the QoS-derived value ("true" for bypassable QoS);
//   - an invalid (non-strict) value is normalized to "false".
//
// Pool/fake entries and sidecars are handled by the caller (sidecars inherit
// from their main).
func recordBypassCapability(info *state.AllocationInfo) {
	if info == nil {
		return
	}
	if info.Annotations == nil {
		info.Annotations = make(map[string]string)
	}
	key := coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey

	// Non-bypassable QoS (dedicated etc.) is always recorded as false.
	if !hasBypassableQoS(info) {
		info.Annotations[key] = bypassCapabilityFalse
		return
	}

	// Bypassable QoS: distinguish missing from invalid.
	val, present := info.Annotations[key]
	if !present {
		// Missing: write QoS-derived value.
		info.Annotations[key] = bypassCapabilityTrue
		return
	}
	switch val {
	case bypassCapabilityTrue, bypassCapabilityFalse:
		// Preserve explicit valid value; never downgrade an explicit false.
	default:
		// Invalid: normalize to false.
		info.Annotations[key] = bypassCapabilityFalse
	}
}

// bypassCapabilityAllow reports whether the allocation is allowed to bypass
// response cpuset backfill. It reads the public annotation strictly AND
// requires a bypassable QoS:
//   - dedicated (and any non-bypassable QoS) is always not allowed;
//   - "true"  -> allowed;
//   - "false"  -> not allowed;
//   - a missing key (legacy checkpoint) derives from QoS for backward compat;
//   - any other non-strict value fails closed to not allowed.
//
// The live EnableBypassCPUSetAdjustment switch is AND-ed on top by the caller, so
// a recorded "true" does not bypass when the live switch is off.
func bypassCapabilityAllow(info *state.AllocationInfo) bool {
	if info == nil || !hasBypassableQoS(info) {
		return false
	}
	val, present := info.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey]
	if !present {
		// Missing key: derive from QoS (legacy checkpoint backward compat).
		return true
	}
	switch val {
	case bypassCapabilityTrue:
		return true
	default:
		// "false" or any invalid value: fail closed.
		return false
	}
}
