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

package controller

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/util/datasource/prometheus"
)

// SPDBaselineSelector names a QoS refinement without introducing a new QoS level.
type SPDBaselineSelector struct {
	QoSLevel           string
	Name               string
	MemoryEnhancements map[string]string
}

var spdBaselineSelectors = []SPDBaselineSelector{
	{
		QoSLevel: apiconsts.PodAnnotationQoSLevelDedicatedCores,
		Name:     "numa_binding_non_exclusive",
		MemoryEnhancements: map[string]string{
			apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
			// Absence must not imply non-exclusive: agents may default numa_exclusive to true.
			apiconsts.PodAnnotationMemoryEnhancementNumaExclusive: "false",
		},
	},
}

func (s SPDBaselineSelector) key() string {
	return s.QoSLevel + ":" + s.Name
}

func BaselineSelectorKeys(qosLevel string, memoryEnhancements map[string]string) []string {
	var keys []string
	for _, selector := range spdBaselineSelectors {
		if selector.QoSLevel != qosLevel {
			continue
		}
		matched := true
		for key, value := range selector.MemoryEnhancements {
			if memoryEnhancements[key] != value {
				matched = false
				break
			}
		}
		if matched {
			keys = append(keys, selector.key())
		}
	}
	return keys
}

func SPDBaselinePercentKeys() []string {
	keys := sets.NewString(
		apiconsts.PodAnnotationQoSLevelSharedCores,
		apiconsts.PodAnnotationQoSLevelDedicatedCores,
		apiconsts.PodAnnotationQoSLevelReclaimedCores,
		apiconsts.PodAnnotationQoSLevelSystemCores,
	)
	for _, selector := range spdBaselineSelectors {
		keys.Insert(selector.key())
	}
	return keys.List()
}

func ValidateSPDBaselinePercent(baselinePercent map[string]int64) error {
	keys := SPDBaselinePercentKeys()
	validKeys := sets.NewString(keys...)
	for key, value := range baselinePercent {
		if !validKeys.Has(key) {
			return fmt.Errorf("invalid --spd-qos-baseline-percent key %q; supported keys: %s", key, strings.Join(keys, ", "))
		}
		if value < apiconsts.SPDBaselinePercentMin || value > apiconsts.SPDBaselinePercentMax {
			return fmt.Errorf("invalid --spd-qos-baseline-percent value for %q: %d; must be in [%d,%d]",
				key, value, apiconsts.SPDBaselinePercentMin, apiconsts.SPDBaselinePercentMax)
		}
	}
	return nil
}

type SPDConfig struct {
	// ReSyncPeriod controls the resync period to generate spd
	ReSyncPeriod time.Duration

	// SPDWorkloadGVResources define those SPD concerned GVRs
	SPDWorkloadGVResources []string
	// SPDPodLabelIndexerKeys are used
	SPDPodLabelIndexerKeys []string

	// EnableCNCCache is to sync spd cnc target config
	EnableCNCCache bool

	IndicatorPlugins []string

	BaselinePercent map[string]int64

	*ResourcePortraitIndicatorPluginConfig
}

// ResourcePortraitIndicatorPluginConfig holds the configurations for resource portrait indicator plugin data.
type ResourcePortraitIndicatorPluginConfig struct {
	// available datasource: prom
	DataSource string
	// DataSourcePromConfig is the prometheus datasource config
	DataSourcePromConfig prometheus.PromConfig
	// ReSyncPeriod controls the resync period to refresh spd
	ReSyncPeriod time.Duration
	// AlgorithmServingAddress is the algorithm serving address
	AlgorithmServingAddress string
	// AlgorithmConfigMapName is the configmap name used by resource portrait plugin
	AlgorithmConfigMapName string
	// AlgorithmConfigMapNamespace is the configmap namespace used by resource portrait plugin
	AlgorithmConfigMapNamespace string
	// EnableAutomaticResyncGlobalConfiguration is used to enable automatic synchronization of global configuration.
	// If this switch is enabled, the plug-in will refresh itself configuration in SPD with the configuration in the
	// global ConfigMap. If this switch is disable, users can customize the configuration in SPD.
	EnableAutomaticResyncGlobalConfiguration bool
}

func NewSPDConfig() *SPDConfig {
	return &SPDConfig{
		BaselinePercent:                       map[string]int64{},
		ResourcePortraitIndicatorPluginConfig: &ResourcePortraitIndicatorPluginConfig{},
	}
}
