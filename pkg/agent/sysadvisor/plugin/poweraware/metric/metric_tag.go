/*
Copyright 2022 The Katalyst Authors.

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

package metric

const (
	metricPowerAwareCurrentPowerInWatt = "node_power_usage"
	metricPowerSpecBudget              = "node_power_budget"
	metricPowerAwareDesiredPowerInWatt = "node_power_advice"
	metricPowerEvictReq                = "node_power_evict_req"
	metricPowerCappingReset            = "node_power_cap_reset"
	metricPowerCappingTarget           = "node_power_cap_target"
	metricPowerCappingCurrent          = "node_power_cap_current"
	metricPowerError                   = "node_power_error"
	metricGetAdviceCalled              = "node_power_get_advice"

	tagPowerAlert         = "alert"
	tagPowerInternalOp    = "internal_op"
	tagPowerActionOp      = "op"
	tagPowerActionMode    = "mode"
	tagPowerCappingOpCode = "op"
	tagErrorCause         = "cause"
)
