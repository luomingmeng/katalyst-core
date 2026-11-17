package eviction

import (
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/eviction"
	"github.com/stretchr/testify/require"
	cliflag "k8s.io/component-base/cli/flag"
)

func TestMemoryPSIWorkloadFlags(t *testing.T) {
	options := NewMemoryPressureEvictionOptions()
	config := eviction.NewMemoryPressureEvictionPluginConfiguration()
	require.NoError(t, options.ApplyTo(config))
	require.Equal(t, []string{"/sys/fs/cgroup/kubepods/burstable"}, config.WorkloadPaths)
	sets := &cliflag.NamedFlagSets{}
	options.AddFlags(sets)
	require.Nil(t, sets.FlagSet("memory-pressure-eviction").Lookup("workload-path"))
	require.NoError(t, sets.FlagSet("memory-pressure-eviction").Parse([]string{"--workload-paths=/cgroup/b,/cgroup/a/,/cgroup/a"}))
	require.NoError(t, options.ApplyTo(config))
	require.Equal(t, []string{"/cgroup/a", "/cgroup/b"}, config.WorkloadPaths)
	// Only the resulting Configuration is normalized; Options remain input.
	require.Equal(t, []string{"/cgroup/b", "/cgroup/a/", "/cgroup/a"}, options.WorkloadPaths)
	// An empty root list would silently disable PSI instead of failing loudly.
	options.WorkloadPaths = nil
	require.Error(t, options.ApplyTo(config))
}

func TestMemoryPSIWorkloadPathsApplyTo(t *testing.T) {
	for _, paths := range [][]string{{"/cgroup/a", "/cgroup/a/child"}, {"relative"}, {""}, {"/cgroup/a/../b"}, {"/", "/cgroup"}} {
		options := NewMemoryPressureEvictionOptions()
		options.WorkloadPaths = paths
		config := eviction.NewMemoryPressureEvictionPluginConfiguration()
		config.WorkloadPaths = []string{"/unchanged"}
		require.Error(t, options.ApplyTo(config))
		require.Equal(t, []string{"/unchanged"}, config.WorkloadPaths)
	}
	options := NewMemoryPressureEvictionOptions()
	options.WorkloadPaths = []string{" /cgroup/a ", "/cgroup/ab"}
	config := eviction.NewMemoryPressureEvictionPluginConfiguration()
	require.NoError(t, options.ApplyTo(config))
	require.Equal(t, []string{"/cgroup/a", "/cgroup/ab"}, config.WorkloadPaths)
}
