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

import dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"

type advisorAttemptConfiguration struct {
	dynamic *dynamicconfig.Configuration
	floor   *dynamicconfig.Configuration
}

type advisorDynamicConfigurationSource interface {
	GetDynamicConfiguration() *dynamicconfig.Configuration
}

// captureAdvisorAttemptConfiguration freezes the dynamic and floor sources
// into an attempt-local configuration that remains immutable for the complete
// advisor operation.
func (p *DynamicPolicy) captureAdvisorAttemptConfiguration() (advisorAttemptConfiguration, error) {
	if p == nil {
		return advisorAttemptConfiguration{}, nil
	}
	if p.dynamicConfig != nil &&
		(p.conf == nil || p.conf.DynamicAgentConfiguration == nil ||
			p.conf.DynamicAgentConfiguration == p.dynamicConfig) {
		return captureAdvisorAttemptConfigurationFrom(p.dynamicConfig)
	}
	if p.dynamicConfig == nil {
		if p.conf == nil || p.conf.DynamicAgentConfiguration == nil {
			return advisorAttemptConfiguration{}, nil
		}
		return captureAdvisorAttemptConfigurationFrom(p.conf)
	}
	dynamic, err := captureAdvisorAttemptConfigurationFrom(p.dynamicConfig)
	if err != nil {
		return advisorAttemptConfiguration{}, err
	}
	floor, err := captureAdvisorAttemptConfigurationFrom(p.conf)
	if err != nil {
		return advisorAttemptConfiguration{}, err
	}
	dynamic.floor = floor.floor
	return dynamic, nil
}

func captureAdvisorAttemptConfigurationFrom(
	source advisorDynamicConfigurationSource,
) (advisorAttemptConfiguration, error) {
	var out advisorAttemptConfiguration
	if source == nil {
		return out, nil
	}
	current := source.GetDynamicConfiguration()
	if current == nil {
		return out, nil
	}
	frozen := cloneAdvisorAttemptDynamicConfiguration(current)
	out.dynamic = frozen
	out.floor = frozen
	return out, nil
}

func cloneAdvisorAttemptDynamicConfiguration(
	current *dynamicconfig.Configuration,
) *dynamicconfig.Configuration {
	if current == nil {
		return nil
	}
	frozen := *current
	if current.AdminQoSConfiguration == nil {
		return &frozen
	}
	adminQoS := *current.AdminQoSConfiguration
	frozen.AdminQoSConfiguration = &adminQoS
	if current.ReclaimedResourceConfiguration != nil {
		reclaimed := *current.ReclaimedResourceConfiguration
		adminQoS.ReclaimedResourceConfiguration = &reclaimed
	}
	if current.QRMPluginConfiguration != nil {
		qrm := *current.QRMPluginConfiguration
		adminQoS.QRMPluginConfiguration = &qrm
		if current.CPUPluginConfiguration != nil {
			cpu := *current.CPUPluginConfiguration
			qrm.CPUPluginConfiguration = &cpu
			cpu.SystemExclusivePool = cloneAdvisorAttemptStringIntMap(current.SystemExclusivePool)
			cpu.SystemExclusivePoolShrinkRatio = cloneFloat64(current.SystemExclusivePoolShrinkRatio)
			cpu.SystemExclusivePoolShrinkMin = cloneInt64(current.SystemExclusivePoolShrinkMin)
			cpu.SystemExclusivePoolShrinkMax = cloneInt64(current.SystemExclusivePoolShrinkMax)
			cpu.BulkheadConfig.BulkheadRDTConfig.ClosCATWays =
				cloneCATWaysMap(current.BulkheadConfig.BulkheadRDTConfig.ClosCATWays)
		}
	}
	if current.AdvisorConfiguration != nil {
		advisor := *current.AdvisorConfiguration
		adminQoS.AdvisorConfiguration = &advisor
		if current.CPUProvisionConfiguration != nil {
			cpuProvision := *current.CPUProvisionConfiguration
			advisor.CPUProvisionConfiguration = &cpuProvision
		}
	}
	return &frozen
}

func cloneAdvisorAttemptStringIntMap(in map[string]int) map[string]int {
	if in == nil {
		return nil
	}
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneFloat64(in *float64) *float64 {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneInt64(in *int64) *int64 {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneCATWaysMap[T any](in map[string]T) map[string]T {
	if in == nil {
		return nil
	}
	out := make(map[string]T, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (p *DynamicPolicy) currentAdvisorAttemptConfiguration() advisorAttemptConfiguration {
	config, err := p.captureAdvisorAttemptConfiguration()
	if err != nil {
		return advisorAttemptConfiguration{}
	}
	return config
}
