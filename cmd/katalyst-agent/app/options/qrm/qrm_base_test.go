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

	"github.com/stretchr/testify/require"

	qrmconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm"
	coreconsts "github.com/kubewharf/katalyst-core/pkg/consts"
)

// TestGenericQRMPluginOptions_ApplyTo_AppendsCustomKeepsDefaults pins the merge
// semantics: user-supplied --pod-annotation-kept-keys are APPENDED to the baseline
// defaults, never replacing them. The public allow key (a default) must survive
// explicit configuration, the custom key must be present, and a random
// never-configured key must NOT be kept.
func TestGenericQRMPluginOptions_ApplyTo_AppendsCustomKeepsDefaults(t *testing.T) {
	t.Parallel()
	as := require.New(t)

	// Baseline defaults already contain the public allow key.
	conf := qrmconfig.NewGenericQRMPluginConfiguration()
	as.Contains(conf.PodAnnotationKeptKeys, coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey)

	// Simulate a user explicitly configuring extra kept annotation keys.
	o := NewGenericQRMPluginOptions()
	o.PodAnnotationKeptKeys = []string{"custom.example.com/keep-me", "custom.example.com/keep-me-too"}
	as.NoError(o.ApplyTo(conf))

	// Default allow key must survive explicit config (append, not replace).
	as.Contains(conf.PodAnnotationKeptKeys, coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey,
		"default allow key must survive explicit kept-keys config")
	// Custom keys must be present.
	as.Contains(conf.PodAnnotationKeptKeys, "custom.example.com/keep-me")
	as.Contains(conf.PodAnnotationKeptKeys, "custom.example.com/keep-me-too")
	// A random, never-configured key must not be kept.
	as.NotContains(conf.PodAnnotationKeptKeys, "random.example.com/not-configured")
}
