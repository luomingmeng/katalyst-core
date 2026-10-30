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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	cliflag "k8s.io/component-base/cli/flag"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	advisorconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/adminqos/advisor"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
	"github.com/kubewharf/katalyst-core/pkg/util/syntax"
)

func TestCPUProvisionOptionsApplyFillDefaultSharePool(t *testing.T) {
	t.Parallel()

	options := NewCPUProvisionOptions()
	options.FillDefaultSharePoolWithNonReclaimCPUs = true

	conf := advisorconfig.NewCPUProvisionConfiguration()
	require.NoError(t, options.ApplyTo(conf))
	require.True(t, conf.FillDefaultSharePoolWithNonReclaimCPUs)
}

func TestCPUProvisionOptionsPreserveSharedReclaimOverlapWhenFillDefaultShareDisabled(t *testing.T) {
	t.Parallel()

	options := NewCPUProvisionOptions()
	options.AllowSharedCoresOverlapReclaimedCores = true
	options.FillDefaultSharePoolWithNonReclaimCPUs = false

	conf := advisorconfig.NewCPUProvisionConfiguration()
	require.NoError(t, options.ApplyTo(conf))
	require.False(t, conf.FillDefaultSharePoolWithNonReclaimCPUs)
	require.True(t, conf.AllowSharedCoresOverlapReclaimedCores)
}

func TestCPUProvisionOptionsFillDefaultShareDisallowsSharedReclaimOverlap(t *testing.T) {
	t.Parallel()

	options := NewCPUProvisionOptions()
	options.AllowSharedCoresOverlapReclaimedCores = true
	options.FillDefaultSharePoolWithNonReclaimCPUs = true

	conf := advisorconfig.NewCPUProvisionConfiguration()
	require.NoError(t, options.ApplyTo(conf))
	require.True(t, conf.FillDefaultSharePoolWithNonReclaimCPUs)
	require.False(t, conf.AllowSharedCoresOverlapReclaimedCores)
	require.True(t, conf.DisableDedicatedCoresOverlapReclaimedCores)
}

func TestCPUProvisionOptionsDynamicConfigurationUsesRequestedOverlapPolicy(t *testing.T) {
	t.Parallel()

	boolPtr := func(value bool) *bool {
		return &value
	}
	tests := []struct {
		name            string
		dynamicAllow    *bool
		dynamicDisable  *bool
		dynamicFill     bool
		expectedAllow   bool
		expectedDisable bool
	}{
		{
			name:            "fill disabled with both overlap fields omitted",
			expectedAllow:   true,
			expectedDisable: false,
		},
		{
			name:            "fill disabled with only allow explicit",
			dynamicAllow:    boolPtr(false),
			expectedAllow:   false,
			expectedDisable: false,
		},
		{
			name:            "fill disabled with only disable explicit",
			dynamicDisable:  boolPtr(true),
			expectedAllow:   true,
			expectedDisable: true,
		},
		{
			name:            "fill disabled with both overlap fields explicit",
			dynamicAllow:    boolPtr(false),
			dynamicDisable:  boolPtr(true),
			expectedAllow:   false,
			expectedDisable: true,
		},
		{
			name:            "fill enabled forces overlap policy",
			dynamicAllow:    boolPtr(true),
			dynamicDisable:  boolPtr(false),
			dynamicFill:     true,
			expectedAllow:   false,
			expectedDisable: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			options := NewCPUProvisionOptions()
			options.AllowSharedCoresOverlapReclaimedCores = true
			options.DisableDedicatedCoresOverlapReclaimedCores = false
			options.FillDefaultSharePoolWithNonReclaimCPUs = true

			conf := dynamic.NewConfiguration()
			require.NoError(t, options.ApplyTo(conf.CPUProvisionConfiguration))
			require.False(t, conf.AllowSharedCoresOverlapReclaimedCores)
			require.True(t, conf.DisableDedicatedCoresOverlapReclaimedCores)

			conf = syntax.DeepCopy(conf).(*dynamic.Configuration)
			conf.ApplyConfiguration(&crd.DynamicConfigCRD{
				AdminQoSConfiguration: &configapi.AdminQoSConfiguration{
					Spec: configapi.AdminQoSConfigurationSpec{
						Config: configapi.AdminQoSConfig{
							AdvisorConfig: &configapi.AdvisorConfig{
								CPUAdvisorConfig: &configapi.CPUAdvisorConfig{
									AllowSharedCoresOverlapReclaimedCores:      tt.dynamicAllow,
									DisableDedicatedCoresOverlapReclaimedCores: tt.dynamicDisable,
									CPUProvisionConfig: &configapi.CPUProvisionConfig{
										FillDefaultSharePoolWithNonReclaimCPUs: boolPtr(tt.dynamicFill),
									},
								},
							},
						},
					},
				},
			})

			require.Equal(t, tt.dynamicFill, conf.FillDefaultSharePoolWithNonReclaimCPUs)
			require.Equal(t, tt.expectedAllow, conf.AllowSharedCoresOverlapReclaimedCores)
			require.Equal(t, tt.expectedDisable, conf.DisableDedicatedCoresOverlapReclaimedCores)
		})
	}
}

const validBoundedConstraintsJSON = `[{"name":"non-reclaimed-cpu-requirement","targetPolicy":"rama","referencePolicy":"dynamic-quota","maxUpperGap":2,"enforce":true}]`

// TestBoundedConstraintsFlagRegisteredOnCPUProvisionOptions proves the flag is
// registered only on the cpu-provision NamedFlagSet, so the agent CLI exposes it
// under the dynamic advisor group rather than the region group. It also proves
// the production wiring: ApplyTo seeds exactly one ControlKnobConstraints entry
// onto the single field (no parallel defaults field).
func TestBoundedConstraintsFlagRegisteredOnCPUProvisionOptions(t *testing.T) {
	t.Parallel()

	o := NewCPUProvisionOptions()
	fss := cliflag.NamedFlagSets{}
	o.AddFlags(&fss)

	fs := fss.FlagSet("cpu-provision")
	require.NotNil(t, fs.Lookup("region-bounded-control-knob-constraints-json"))
	require.NoError(t, fs.Parse([]string{"--region-bounded-control-knob-constraints-json", validBoundedConstraintsJSON}))

	conf := advisorconfig.NewCPUProvisionConfiguration()
	require.NoError(t, o.ApplyTo(conf))
	require.Len(t, conf.ControlKnobConstraints, 1)
	assertBoundedEntry(t, conf.ControlKnobConstraints[0])
}

func assertBoundedEntry(t *testing.T, e configapi.ControlKnobConstraints) {
	t.Helper()
	require.Equal(t, configapi.ControlKnobName("non-reclaimed-cpu-requirement"), e.Name)
	require.Equal(t, configapi.CPUProvisionPolicyName("rama"), e.TargetPolicy)
	require.Equal(t, configapi.CPUProvisionPolicyName("dynamic-quota"), e.ReferencePolicy)
	require.NotNil(t, e.MaxUpperGap)
	require.Equal(t, 2.0, *e.MaxUpperGap)
	require.NotNil(t, e.Enforce)
	require.True(t, *e.Enforce)
}

// TestBoundedConstraintsEmptyFlagLeavesEmptyConstraints: an absent/empty flag
// must seed an empty non-nil constraints slice, with no error.
func TestBoundedConstraintsEmptyFlagLeavesEmptyConstraints(t *testing.T) {
	t.Parallel()

	o := NewCPUProvisionOptions()
	conf := advisorconfig.NewCPUProvisionConfiguration()
	require.NoError(t, o.ApplyTo(conf))
	require.Empty(t, conf.ControlKnobConstraints)
}

// TestBoundedConstraintsStrictDecodeFailure covers the strict streaming decode:
// malformed JSON, trailing data, unknown field, and non-array payload all fail.
func TestBoundedConstraintsStrictDecodeFailure(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"malformed":     `[{"name":"x","targetPolicy":"rama"`,
		"trailing data": `[{"name":"x","targetPolicy":"rama","referencePolicy":"canonical","maxUpperGap":1}] {"name":"y"}`,
		"unknown field": `[{"name":"x","targetPolicy":"rama","referencePolicy":"canonical","maxUpperGap":1,"bogus":true}]`,
		"not an array":  `{"name":"x","targetPolicy":"rama","referencePolicy":"canonical","maxUpperGap":1}`,
	}
	for name, raw := range cases {
		raw := raw
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := NewCPUProvisionOptions()
			o.BoundedControlKnobConstraintsJSON = raw
			conf := advisorconfig.NewCPUProvisionConfiguration()
			require.Error(t, o.ApplyTo(conf))
			// A failed decode must not seed half-parsed constraints.
			require.Empty(t, conf.ControlKnobConstraints)
		})
	}
}

// TestBoundedConstraintsValidationFailure covers the startup invariants: empty
// name, name-only, one-sided (half-explicit), negative tolerance, and an entry
// with no positive tolerance.
func TestBoundedConstraintsValidationFailure(t *testing.T) {
	t.Parallel()

	base := func(mutate func(map[string]interface{})) string {
		e := map[string]interface{}{
			"name":            "x",
			"targetPolicy":    "rama",
			"referencePolicy": "canonical",
			"maxUpperGap":     2.0,
		}
		mutate(e)
		b, err := json.Marshal([]map[string]interface{}{e})
		require.NoError(t, err)
		return string(b)
	}

	cases := map[string]string{
		"empty name":            base(func(e map[string]interface{}) { e["name"] = "" }),
		"name-only rejected":    base(func(e map[string]interface{}) { delete(e, "targetPolicy"); delete(e, "referencePolicy") }),
		"one-sided":             base(func(e map[string]interface{}) { delete(e, "referencePolicy") }),
		"negative tolerance":    base(func(e map[string]interface{}) { e["maxUpperGap"] = -1.0 }),
		"no positive tolerance": base(func(e map[string]interface{}) { e["maxUpperGap"] = 0.0 }),
	}
	for name, raw := range cases {
		raw := raw
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := NewCPUProvisionOptions()
			o.BoundedControlKnobConstraintsJSON = raw
			conf := advisorconfig.NewCPUProvisionConfiguration()
			require.Error(t, o.ApplyTo(conf))
			require.Empty(t, conf.ControlKnobConstraints)
		})
	}
}
