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

package qrm

import (
	"testing"

	"github.com/stretchr/testify/require"
	cliflag "k8s.io/component-base/cli/flag"

	qrmconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm"
)

func TestCPUOptionsAddFlagsSNBCPUTotalRequestThresholdRatio(t *testing.T) {
	t.Parallel()

	options := NewCPUOptions()
	fss := &cliflag.NamedFlagSets{}
	options.AddFlags(fss)

	fs := fss.FlagSet("cpu_resource_plugin")
	flag := fs.Lookup("snb-cpu-total-request-threshold-ratio")
	require.NotNil(t, flag)
	require.Equal(t, "0", flag.DefValue)

	require.NoError(t, fs.Set("snb-cpu-total-request-threshold-ratio", "0.75"))
	require.Equal(t, 0.75, options.SNBCPUTotalRequestThresholdRatio)
}

func TestCPUOptionsApplyToSNBCPUTotalRequestThresholdRatio(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		ratio   float64
		wantErr bool
	}{
		{name: "disabled", ratio: 0},
		{name: "enabled", ratio: 0.5},
		{name: "negative", ratio: -0.1, wantErr: true},
		{name: "greater than one", ratio: 1.1, wantErr: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			options := NewCPUOptions()
			options.SNBCPUTotalRequestThresholdRatio = tc.ratio
			conf := qrmconfig.NewCPUQRMPluginConfig()
			err := options.ApplyTo(conf)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.ratio, conf.SNBCPUTotalRequestThresholdRatio)
		})
	}
}
