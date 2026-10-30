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

package advisor

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
	"github.com/kubewharf/katalyst-core/pkg/util/syntax"
)

func TestCPUProvisionConfigurationFillDefaultSharePool(t *testing.T) {
	t.Parallel()

	t.Run("default disabled", func(t *testing.T) {
		t.Parallel()

		conf := NewCPUProvisionConfiguration()
		require.False(t, conf.FillDefaultSharePoolWithNonReclaimCPUs)
	})

	t.Run("apply dynamic configuration", func(t *testing.T) {
		t.Parallel()

		enabled := true
		conf := NewCPUProvisionConfiguration()
		conf.ApplyConfiguration(&crd.DynamicConfigCRD{
			AdminQoSConfiguration: &configapi.AdminQoSConfiguration{
				Spec: configapi.AdminQoSConfigurationSpec{
					Config: configapi.AdminQoSConfig{
						AdvisorConfig: &configapi.AdvisorConfig{
							CPUAdvisorConfig: &configapi.CPUAdvisorConfig{
								CPUProvisionConfig: &configapi.CPUProvisionConfig{
									FillDefaultSharePoolWithNonReclaimCPUs: &enabled,
								},
							},
						},
					},
				},
			},
		})

		require.True(t, conf.FillDefaultSharePoolWithNonReclaimCPUs)
	})
}

func TestCPUProvisionConfigurationPreservesSharedReclaimOverlapWhenFillDefaultShareDisabled(t *testing.T) {
	t.Parallel()

	enabled := true
	disabled := false
	conf := NewCPUProvisionConfiguration()
	conf.ApplyConfiguration(&crd.DynamicConfigCRD{
		AdminQoSConfiguration: &configapi.AdminQoSConfiguration{
			Spec: configapi.AdminQoSConfigurationSpec{
				Config: configapi.AdminQoSConfig{
					AdvisorConfig: &configapi.AdvisorConfig{
						CPUAdvisorConfig: &configapi.CPUAdvisorConfig{
							AllowSharedCoresOverlapReclaimedCores: &enabled,
							CPUProvisionConfig: &configapi.CPUProvisionConfig{
								FillDefaultSharePoolWithNonReclaimCPUs: &disabled,
							},
						},
					},
				},
			},
		},
	})

	require.False(t, conf.FillDefaultSharePoolWithNonReclaimCPUs)
	require.True(t, conf.AllowSharedCoresOverlapReclaimedCores)
}

func TestCPUProvisionConfigurationFillDefaultShareDisallowsSharedReclaimOverlap(t *testing.T) {
	t.Parallel()

	enabled := true
	conf := NewCPUProvisionConfiguration()
	conf.ApplyConfiguration(&crd.DynamicConfigCRD{
		AdminQoSConfiguration: &configapi.AdminQoSConfiguration{
			Spec: configapi.AdminQoSConfigurationSpec{
				Config: configapi.AdminQoSConfig{
					AdvisorConfig: &configapi.AdvisorConfig{
						CPUAdvisorConfig: &configapi.CPUAdvisorConfig{
							AllowSharedCoresOverlapReclaimedCores: &enabled,
							CPUProvisionConfig: &configapi.CPUProvisionConfig{
								FillDefaultSharePoolWithNonReclaimCPUs: &enabled,
							},
						},
					},
				},
			},
		},
	})

	require.True(t, conf.FillDefaultSharePoolWithNonReclaimCPUs)
	require.False(t, conf.AllowSharedCoresOverlapReclaimedCores)
	require.True(t, conf.DisableDedicatedCoresOverlapReclaimedCores)
}

func ptr(f float64) *float64 { return &f }

// seedStartupA builds the production startup state: a CPUProvisionConfiguration
// whose ControlKnobConstraints carries the flag-seeded set A. This is what the
// KCC manager's defaultConfig snapshot holds after Options.ApplyTo.
func seedStartupA() *CPUProvisionConfiguration {
	seed := NewCPUProvisionConfiguration()
	seed.ControlKnobConstraints = []configapi.ControlKnobConstraints{
		{Name: configapi.ControlKnobNonReclaimedCPURequirement, TargetPolicy: "rama", ReferencePolicy: "dynamic-quota",
			RestrictConstraints: configapi.RestrictConstraints{MaxUpperGap: ptr(2)}},
	}
	return seed
}

// round simulates one KCC updateConfig cycle: deep-copy the startup defaultConfig
// (the public path the manager uses via syntax.DeepCopy), then ApplyConfiguration
// once with the given AQC. It returns the fresh per-round object.
func round(seed *CPUProvisionConfiguration, aqc *configapi.AdminQoSConfiguration) *CPUProvisionConfiguration {
	current := syntax.DeepCopy(seed).(*CPUProvisionConfiguration)
	current.ApplyConfiguration(&crd.DynamicConfigCRD{AdminQoSConfiguration: aqc})
	return current
}

// aqcWithConstraints wraps the given Constraints pointer into an AQC CRD the way
// the informer would hand it to ApplyConfiguration.
func aqcWithConstraints(constraints *[]configapi.ControlKnobConstraints) *configapi.AdminQoSConfiguration {
	return &configapi.AdminQoSConfiguration{
		Spec: configapi.AdminQoSConfigurationSpec{
			Config: configapi.AdminQoSConfig{
				AdvisorConfig: &configapi.AdvisorConfig{
					CPUAdvisorConfig: &configapi.CPUAdvisorConfig{
						CPUProvisionConfig: &configapi.CPUProvisionConfig{Constraints: constraints},
					},
				},
			},
		},
	}
}

func TestCPUProvisionConfigurationControlKnobConstraintsTriState(t *testing.T) {
	t.Parallel()

	nrKnob := configapi.ControlKnobNonReclaimedCPURequirement
	quotaKnob := configapi.ControlKnobReclaimedCoresCPUQuota

	t.Run("absent AQC keeps startup A", func(t *testing.T) {
		t.Parallel()
		current := round(seedStartupA(), nil)
		require.Len(t, current.ControlKnobConstraints, 1)
		require.Equal(t, nrKnob, current.ControlKnobConstraints[0].Name)
		require.Equal(t, 2.0, *current.ControlKnobConstraints[0].MaxUpperGap)
	})

	t.Run("CPUProvisionConfig present but Constraints nil keeps A", func(t *testing.T) {
		t.Parallel()
		current := round(seedStartupA(), aqcWithConstraints(nil))
		require.Len(t, current.ControlKnobConstraints, 1)
		require.Equal(t, nrKnob, current.ControlKnobConstraints[0].Name)
	})

	t.Run("empty slice clears all bounds", func(t *testing.T) {
		t.Parallel()
		empty := []configapi.ControlKnobConstraints{}
		current := round(seedStartupA(), aqcWithConstraints(&empty))
		require.NotNil(t, current.ControlKnobConstraints)
		require.Empty(t, current.ControlKnobConstraints)
	})

	t.Run("non-empty B whole-replaces A with no merge", func(t *testing.T) {
		t.Parallel()
		replacement := []configapi.ControlKnobConstraints{
			{Name: quotaKnob, TargetPolicy: "rama", ReferencePolicy: "canonical",
				RestrictConstraints: configapi.RestrictConstraints{MaxLowerGap: ptr(5)}},
		}
		current := round(seedStartupA(), aqcWithConstraints(&replacement))
		require.Len(t, current.ControlKnobConstraints, 1)
		require.Equal(t, quotaKnob, current.ControlKnobConstraints[0].Name)
	})

	t.Run("deletion next round restores A from deep-copied defaultConfig", func(t *testing.T) {
		t.Parallel()
		seed := seedStartupA()

		// Round 1: B overrides A.
		replacement := []configapi.ControlKnobConstraints{
			{Name: quotaKnob, TargetPolicy: "rama", ReferencePolicy: "canonical",
				RestrictConstraints: configapi.RestrictConstraints{MaxLowerGap: ptr(5)}},
		}
		first := round(seed, aqcWithConstraints(&replacement))
		require.Equal(t, quotaKnob, first.ControlKnobConstraints[0].Name)

		// Round 2: AQC deleted (nil). A fresh deep-copy of the startup seed must
		// re-enter Apply with A, and the nil override leaves A in place.
		second := round(seed, nil)
		require.Len(t, second.ControlKnobConstraints, 1)
		require.Equal(t, nrKnob, second.ControlKnobConstraints[0].Name)
		require.Equal(t, 2.0, *second.ControlKnobConstraints[0].MaxUpperGap)
	})

	t.Run("slice preserved verbatim, order and entries intact", func(t *testing.T) {
		t.Parallel()
		in := []configapi.ControlKnobConstraints{
			{Name: nrKnob, RestrictConstraints: configapi.RestrictConstraints{MaxUpperGap: ptr(2)}}, // omitted target
			{Name: quotaKnob, TargetPolicy: "rama", ReferencePolicy: "dynamic-quota", RestrictConstraints: configapi.RestrictConstraints{MaxUpperGap: ptr(3)}},
		}
		current := round(seedStartupA(), aqcWithConstraints(&in))
		require.Len(t, current.ControlKnobConstraints, 2)
		require.Equal(t, nrKnob, current.ControlKnobConstraints[0].Name)
		require.Equal(t, quotaKnob, current.ControlKnobConstraints[1].Name)
	})

	t.Run("deep copy does not alias informer-owned Enforce/gap/ratio pointers", func(t *testing.T) {
		t.Parallel()
		enforceTrue := true
		upper := 2.0
		upperRatio := 0.1
		in := []configapi.ControlKnobConstraints{
			{Name: nrKnob, TargetPolicy: "rama", ReferencePolicy: "canonical",
				RestrictConstraints: configapi.RestrictConstraints{MaxUpperGap: &upper, MaxUpperGapRatio: &upperRatio},
				Enforce:             &enforceTrue},
		}
		current := round(seedStartupA(), aqcWithConstraints(&in))
		got := current.ControlKnobConstraints[0]
		require.NotNil(t, got.MaxUpperGap)
		require.NotSame(t, in[0].MaxUpperGap, got.MaxUpperGap)
		require.NotSame(t, in[0].MaxUpperGapRatio, got.MaxUpperGapRatio)
		require.NotSame(t, in[0].Enforce, got.Enforce)
		// Mutating the source must not affect the stored copy.
		*in[0].MaxUpperGap = 99
		*in[0].MaxUpperGapRatio = 0.9
		*in[0].Enforce = false
		require.Equal(t, 2.0, *got.MaxUpperGap)
		require.Equal(t, 0.1, *got.MaxUpperGapRatio)
		require.True(t, *got.Enforce)
	})

	t.Run("mutating stored slice does not alias the startup seed", func(t *testing.T) {
		t.Parallel()
		seed := seedStartupA()
		current := round(seed, nil)
		current.ControlKnobConstraints[0].MaxUpperGap = ptr(999)
		require.Equal(t, 2.0, *seed.ControlKnobConstraints[0].MaxUpperGap)
	})

	t.Run("multiple name-only entries warn exactly once per ApplyConfiguration", func(t *testing.T) {
		t.Parallel()
		// klog.SetOutput is the repo's existing lightweight log-capture hook
		// (see pkg/util/resource-recommend/log/logger_test.go). We point it at a
		// buffer, run one ApplyConfiguration carrying several name-only entries,
		// and expect a single "name-only" warning line rather than one per entry.
		var buf bytes.Buffer
		klog.LogToStderr(false)
		klog.SetOutput(&buf)
		defer func() {
			klog.SetOutput(nil)
			klog.LogToStderr(true)
		}()

		nameOnly := []configapi.ControlKnobConstraints{
			{Name: "knob-a", RestrictConstraints: configapi.RestrictConstraints{MaxUpperGap: ptr(2)}},
			{Name: "knob-b", RestrictConstraints: configapi.RestrictConstraints{MaxLowerGap: ptr(2)}},
			{Name: "knob-c"},
		}
		round(seedStartupA(), aqcWithConstraints(&nameOnly))
		klog.Flush()

		// klog v2 emits each record twice to a SetOutput buffer (verified empirically),
		// so one Warningf call shows as 2 lines. Three name-only entries must therefore
		// yield 2 lines — i.e. exactly one warning call, not one per entry (which would
		// be 6 lines).
		require.Equal(t, 2, strings.Count(buf.String(), "name-only"),
			"one warning call for three name-only entries (klog doubles each record), got:\n%s", buf.String())
	})
}
