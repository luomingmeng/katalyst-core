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

package finegrainedresource

import (
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
)

type CPUWeightConfiguration struct {
	RestoreRules  []CPUWeightRestore
	OverrideRules []CPUWeightOverride
}

type CPUWeightRestore struct {
	Name        string
	PodSelector string
}

type CPUWeightOverride struct {
	Name         string
	PodSelector  string
	PodCPUDemand int64
}

func NewCPUWeightConfiguration() *CPUWeightConfiguration {
	return &CPUWeightConfiguration{}
}

func (c *CPUWeightConfiguration) ApplyConfiguration(conf *crd.DynamicConfigCRD) {
	if aqc := conf.AdminQoSConfiguration; aqc != nil &&
		aqc.Spec.Config.FineGrainedResourceConfig != nil &&
		aqc.Spec.Config.FineGrainedResourceConfig.CPUWeightConfig != nil {

		kccConfig := aqc.Spec.Config.FineGrainedResourceConfig.CPUWeightConfig

		var restoreRules []CPUWeightRestore
		for _, r := range kccConfig.RestoreRules {
			restoreRules = append(restoreRules, CPUWeightRestore{
				Name:        r.Name,
				PodSelector: r.PodSelector,
			})
		}
		c.RestoreRules = restoreRules

		var overrideRules []CPUWeightOverride
		for _, r := range kccConfig.OverrideRules {
			overrideRules = append(overrideRules, CPUWeightOverride{
				Name:         r.Name,
				PodSelector:  r.PodSelector,
				PodCPUDemand: r.PodCPUDemand,
			})
		}
		c.OverrideRules = overrideRules
	}
}
