package eviction

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryPressureConfigurationDoesNotAddDefaults(t *testing.T) {
	require.Empty(t, NewMemoryPressureEvictionPluginConfiguration().WorkloadPaths)
}
