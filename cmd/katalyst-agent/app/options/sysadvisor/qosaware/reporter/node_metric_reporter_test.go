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

package reporter

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	reporterconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/sysadvisor/qosaware/reporter"
)

func TestNodeMetricIncludeShmem(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		args []string
		want bool
	}{
		{name: "default preserves memory accounting"},
		{name: "include shmem", args: []string{"--node-metric-reporter-include-shmem=true"}, want: true},
		{name: "explicitly exclude shmem", args: []string{"--node-metric-reporter-include-shmem=false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := NewNodeMetricReporterOptions()
			flags := pflag.NewFlagSet(tc.name, pflag.ContinueOnError)
			opts.AddFlags(flags)
			require.NoError(t, flags.Parse(tc.args))
			conf := reporterconfig.NewNodeMetricReporterConfiguration()
			require.NoError(t, opts.ApplyTo(conf))
			flag := flags.Lookup("node-metric-reporter-include-shmem")
			require.NotNil(t, flag)
			require.Equal(t, tc.want, conf.IncludeShmem)
		})
	}
}

func TestNodeMetricDisableGroupMetric(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		args []string
		want bool
	}{
		{name: "default keeps group metrics"},
		{name: "disable group metrics", args: []string{"--node-metric-reporter-disable-group-metric=true"}, want: true},
		{name: "explicitly keep group metrics", args: []string{"--node-metric-reporter-disable-group-metric=false"}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := NewNodeMetricReporterOptions()
			flags := pflag.NewFlagSet(tc.name, pflag.ContinueOnError)
			opts.AddFlags(flags)
			require.NoError(t, flags.Parse(tc.args))
			conf := reporterconfig.NewNodeMetricReporterConfiguration()
			require.NoError(t, opts.ApplyTo(conf))
			require.Equal(t, tc.want, conf.DisableGroupMetric)
		})
	}
}
