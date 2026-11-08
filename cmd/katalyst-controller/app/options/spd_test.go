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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cliflag "k8s.io/component-base/cli/flag"

	"github.com/kubewharf/katalyst-core/pkg/config/controller"
)

func TestSPDBaselinePercentOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		value   string
		want    map[string]int64
		wantErr string
	}{
		{name: "empty", want: map[string]int64{}},
		{name: "qos defaults", value: "shared_cores=0,dedicated_cores=100,reclaimed_cores=25,system_cores=75",
			want: map[string]int64{"shared_cores": 0, "dedicated_cores": 100, "reclaimed_cores": 25, "system_cores": 75}},
		{name: "selector override", value: "shared_cores=0,dedicated_cores=0,dedicated_cores:numa_binding_non_exclusive=100",
			want: map[string]int64{"shared_cores": 0, "dedicated_cores": 0, "dedicated_cores:numa_binding_non_exclusive": 100}},
		{name: "selector without qos default", value: "dedicated_cores:numa_binding_non_exclusive=0",
			want: map[string]int64{"dedicated_cores:numa_binding_non_exclusive": 0}},
		{name: "unknown qos", value: "dedicated_core=0", wantErr: "dedicated_core"},
		{name: "unknown selector", value: "dedicated_cores:numa_binding_nonexclusive=100", wantErr: "numa_binding_nonexclusive"},
		{name: "selector under wrong qos", value: "shared_cores:numa_binding_non_exclusive=100", wantErr: "shared_cores:numa_binding_non_exclusive"},
		{name: "negative qos value", value: "shared_cores=-1", wantErr: "[0,100]"},
		{name: "qos value above maximum", value: "dedicated_cores=101", wantErr: "[0,100]"},
		{name: "negative selector value", value: "dedicated_cores:numa_binding_non_exclusive=-1", wantErr: "[0,100]"},
		{name: "selector value above maximum", value: "dedicated_cores:numa_binding_non_exclusive=101", wantErr: "[0,100]"},
		{name: "value overflows int32", value: "shared_cores=4294967296", wantErr: "[0,100]"},
		{name: "key whitespace", value: " shared_cores=0", wantErr: " shared_cores"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := NewSPDOptions()
			fss := &cliflag.NamedFlagSets{}
			o.AddFlags(fss)
			if tt.value != "" {
				require.NoError(t, fss.FlagSet("spd").Parse([]string{"--spd-qos-baseline-percent=" + tt.value}))
			}
			c := controller.NewSPDConfig()
			err := o.ApplyTo(c)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Contains(t, err.Error(), "spd-qos-baseline-percent")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, c.BaselinePercent)
		})
	}
}
