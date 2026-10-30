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

package region

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	cpuregion "github.com/kubewharf/katalyst-core/pkg/config/agent/sysadvisor/qosaware/resource/cpu/region"
)

func TestCPURegionOptionsExactRestrictRefPolicy(t *testing.T) {
	o := NewCPURegionOptions()
	// Default remains rama -> dynamic-quota.
	c := cpuregion.NewCPURegionConfiguration()
	assert.NoError(t, o.ApplyTo(c))
	assert.Equal(t, types.CPUProvisionPolicyDynamicQuota, c.ExactRestrictRefPolicy["rama"])

	// Override end-to-end via the flag name.
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)
	assert.NoError(t, fs.Parse([]string{"--region-exact-restrict-ref-policy", "rama=canonical"}))
	c2 := cpuregion.NewCPURegionConfiguration()
	assert.NoError(t, o.ApplyTo(c2))
	assert.Equal(t, types.CPUProvisionPolicyCanonical, c2.ExactRestrictRefPolicy["rama"])
}

func TestOldRestrictRefFlagAbsent(t *testing.T) {
	o := NewCPURegionOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)
	// The old flag name must not be registered.
	assert.Nil(t, fs.Lookup("region-restrict-ref-policy"))
	assert.NotNil(t, fs.Lookup("region-exact-restrict-ref-policy"))
}

// TestBoundedFlagAbsentOnRegionOptions guards the Core migration: the bounded
// control-knob constraints flag/parse must live on CPUProvisionOptions (dynamic
// advisor), never re-registered or re-parsed here. A compile-time guarantee plus
// this Lookup check covers both.
func TestBoundedFlagAbsentOnRegionOptions(t *testing.T) {
	o := NewCPURegionOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)
	require.Nil(t, fs.Lookup("region-bounded-control-knob-constraints-json"))

	c := cpuregion.NewCPURegionConfiguration()
	require.NoError(t, o.ApplyTo(c))
}
