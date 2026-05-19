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

package eviction

import (
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
)

// EvictionExplicitTriggerAnnotationValue is the constant value applied to the
// explicit-trigger annotation that katalyst stamps on Eviction API objects.
// It is intentionally fixed (not configurable) so that higher-level
// components can match on the presence of the explicit-trigger key alone.
const EvictionExplicitTriggerAnnotationValue = "true"

// EvictionExplicitTriggerAnnotationConfig declares when and how katalyst-agent
// should stamp an explicit-trigger annotation onto the Eviction API object.
//
// The explicit-trigger annotation is the contract between the agent and any
// higher-level component (controller, operator, or external automation)
// that needs to react to an eviction — for example to reschedule, repair,
// escalate, or notify. By writing the explicit-trigger key onto the
// Eviction object itself (rather than relying on the pod's annotations,
// which disappear the moment the pod is deleted), upstream consumers can
// observe and act on the eviction via the eviction subresource, admission
// webhooks, or audit logs.
//
// The rule is "fire if any single (key, value) pair on the pod matches":
//   - keys are OR-combined (any configured key on the pod is enough),
//   - values within a key are OR-combined (any allowed value matches).
//
// The rule is inactive when ExplicitTriggerAnnotationKey is empty or
// ExplicitlyTriggeringPodAnnotations is empty, so the feature is opt-in.
type EvictionExplicitTriggerAnnotationConfig struct {
	// ExplicitlyTriggeringPodAnnotations maps a pod-annotation key to the set of
	// values that should cause the explicit-trigger annotation to be stamped on
	// the Eviction object. When the evicted pod has any one of these keys set
	// to any one of that key's listed values, the rule fires.
	ExplicitlyTriggeringPodAnnotations map[string]sets.String
	// ExplicitTriggerAnnotationKey is the annotation key written onto the
	// Eviction object when the rule fires. The corresponding value is always
	// EvictionExplicitTriggerAnnotationValue. Higher-level components should
	// match on this key.
	ExplicitTriggerAnnotationKey string
}

type GenericEvictionConfiguration struct {
	// Inner plugins is the list of plugins implemented in katalyst to enable or disable
	// '*' means "all enabled by default"
	// 'foo' means "enable 'foo'"
	// '-foo' means "disable 'foo'"
	// first item for a particular name wins
	InnerPlugins []string

	// ConditionTransitionPeriod is duration the eviction manager has to wait before transitioning out of a condition.
	ConditionTransitionPeriod time.Duration

	// EvictionManagerSyncPeriod is the interval duration that eviction manager fetches information from registered plugins
	EvictionManagerSyncPeriod time.Duration

	// those two variables are used to filter out eviction-free pods
	EvictionSkippedAnnotationKeys sets.String
	EvictionSkippedLabelKeys      sets.String

	// EvictionBurst limit the burst eviction counts
	EvictionBurst int

	// PodKiller specify the pod killer implementation
	PodKiller string

	// QoSPodKillers specify the pod killer implementation for different QoS levels
	QoSPodKillers map[string]string

	// StrictAuthentication means whether to authenticate plugins strictly
	StrictAuthentication bool

	// PodMetricLabels defines the pod labels to be added in metric selector lists
	PodMetricLabels sets.String

	// RecordManager specifies the eviction record manager to use
	RecordManager string

	// HostPathNotifierRootPath
	HostPathNotifierRootPath string

	// *EvictionExplicitTriggerAnnotationConfig configures the marker annotation
	// that katalyst stamps onto Eviction API objects so that higher-level
	// components can react to specific evictions. Inactive by default.
	*EvictionExplicitTriggerAnnotationConfig
}

type EvictionConfiguration struct {
	*ReclaimedResourcesEvictionConfiguration
	*MemoryPressureEvictionConfiguration
	*CPUPressureEvictionConfiguration
}

func NewGenericEvictionConfiguration() *GenericEvictionConfiguration {
	return &GenericEvictionConfiguration{
		EvictionSkippedAnnotationKeys:           sets.NewString(),
		EvictionSkippedLabelKeys:                sets.NewString(),
		PodMetricLabels:                         sets.NewString(),
		EvictionExplicitTriggerAnnotationConfig: NewEvictionExplicitTriggerAnnotationConfig(),
	}
}

func NewEvictionExplicitTriggerAnnotationConfig() *EvictionExplicitTriggerAnnotationConfig {
	return &EvictionExplicitTriggerAnnotationConfig{
		ExplicitlyTriggeringPodAnnotations: map[string]sets.String{},
	}
}

func NewEvictionConfiguration() *EvictionConfiguration {
	return &EvictionConfiguration{
		ReclaimedResourcesEvictionConfiguration: NewReclaimedResourcesEvictionConfiguration(),
		MemoryPressureEvictionConfiguration:     NewMemoryPressureEvictionPluginConfiguration(),
		CPUPressureEvictionConfiguration:        NewCPUPressureEvictionConfiguration(),
	}
}
