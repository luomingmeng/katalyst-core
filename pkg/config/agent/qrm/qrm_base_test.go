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

package qrm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	katalystapiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	coreconsts "github.com/kubewharf/katalyst-core/pkg/consts"
)

func TestNewGenericQRMPluginConfiguration_ContainsContainerCPUIdleAnnotation(t *testing.T) {
	t.Parallel()

	conf := NewGenericQRMPluginConfiguration()
	assert.Contains(t, conf.PodAnnotationKeptKeys, katalystapiconsts.PodAnnotationContainerCPUIdleRateKey)
}

// TestNewGenericQRMPluginConfiguration_DefaultKeepsAllowBypassAnnotation proves the
// public bypass-capability annotation is part of the baseline kept-annotation set,
// so it survives the QRM annotation filter at admission without a per-call-site helper.
// It references the neutral core consts package, keeping the config layer free of any
// CPU-plugin dependency.
func TestNewGenericQRMPluginConfiguration_DefaultKeepsAllowBypassAnnotation(t *testing.T) {
	t.Parallel()

	conf := NewGenericQRMPluginConfiguration()
	assert.Contains(t, conf.PodAnnotationKeptKeys, coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey)
}

func TestNewQRMPluginsConfiguration_ResctrlConfigHasNoOptionDefaults(t *testing.T) {
	t.Parallel()

	require.Empty(t, NewQRMPluginsConfiguration().ResctrlConfig.DefaultClosIDs)
}
