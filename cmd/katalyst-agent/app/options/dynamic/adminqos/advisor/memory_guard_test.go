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
	"testing"

	"github.com/stretchr/testify/require"
	cliflag "k8s.io/component-base/cli/flag"

	advisorconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/adminqos/advisor"
)

func TestNewMemoryGuardOptions(t *testing.T) {
	t.Parallel()

	require.Equal(t, "low", NewMemoryGuardOptions().CriticalWatermarkSource)
}

func TestMemoryGuardOptionsAddFlags(t *testing.T) {
	t.Parallel()

	fss := &cliflag.NamedFlagSets{}
	NewMemoryGuardOptions().AddFlags(fss)

	require.NotNil(t, fss.FlagSet("memory-guard").Lookup("memory-advisor-critical-watermark-source"))
}

func TestMemoryGuardOptionsApplyTo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		source  string
		want    string
		wantErr bool
	}{
		{name: "low", source: "low", want: "low"},
		{name: "high", source: "high", want: "high"},
		{name: "empty", source: "", want: "low"},
		{name: "uppercase", source: "LOW", wantErr: true},
		{name: "invalid", source: "critical", wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			options := NewMemoryGuardOptions()
			options.CriticalWatermarkSource = tt.source
			conf := advisorconfig.NewMemoryGuardConfiguration()

			err := options.ApplyTo(conf)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, conf.CriticalWatermarkSource)
		})
	}
}
