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

package options

import (
	"testing"

	"github.com/stretchr/testify/require"
	cliflag "k8s.io/component-base/cli/flag"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
	"github.com/kubewharf/katalyst-core/pkg/util/syntax"
)

func TestOptionsInheritStartupDisableRDTToDynamicConfig(t *testing.T) {
	t.Parallel()
	options := NewOptions()
	fss := cliflag.NamedFlagSets{}
	options.AddFlags(&fss)
	require.NoError(t, fss.FlagSet("resctrl").Parse([]string{"--disable-rdt=true"}))

	conf, err := options.Config()
	require.NoError(t, err)
	require.True(t, conf.ResctrlConfig.DisableRDT)
	require.True(t, conf.GetDynamicConfiguration().RDTConfig.DisableRDT)
}

// TestAgentOptionsWiresBoundedConstraintsIntoDynamicConfig is the production
// wiring test: it goes through the real top-level Options assembly (AddFlags on
// NamedFlagSets, then Config/ApplyTo) rather than hand-calling setters. It proves
// the startup flag seeds the dynamic CPUProvisionConfiguration.ControlKnobConstraints
// as the startup default, and that an AQC omitting CPUProvisionConfig keeps that
// default on a fresh per-round deep copy (the KCC manager lifecycle).
func TestAgentOptionsWiresBoundedConstraintsIntoDynamicConfig(t *testing.T) {
	t.Parallel()
	options := NewOptions()
	fss := cliflag.NamedFlagSets{}
	options.AddFlags(&fss)
	valid := `[{"name":"non-reclaimed-cpu-requirement","targetPolicy":"rama","referencePolicy":"dynamic-quota","maxUpperGap":2,"enforce":true}]`
	require.NoError(t, fss.FlagSet("cpu-provision").Parse([]string{"--region-bounded-control-knob-constraints-json", valid}))

	conf, err := options.Config()
	require.NoError(t, err)
	dynConf := conf.GetDynamicConfiguration()
	require.Len(t, dynConf.CPUProvisionConfiguration.ControlKnobConstraints, 1)

	// A nil/absent AQC on a fresh per-round deep copy (the exact KCC manager
	// pattern: syntax.DeepCopy(defaultConfig) then ApplyConfiguration) must leave
	// the startup default intact, not empty the slice.
	round := syntax.DeepCopy(dynConf).(*dynamic.Configuration)
	round.ApplyConfiguration(&crd.DynamicConfigCRD{})
	require.Len(t, round.CPUProvisionConfiguration.ControlKnobConstraints, 1)
}
