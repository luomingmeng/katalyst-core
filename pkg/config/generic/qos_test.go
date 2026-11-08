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

package generic

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
)

func TestGetQoSEnhancementKVs(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		value string
		want  map[string]string
	}{
		{name: "missing", want: map[string]string{}},
		{name: "valid", value: `{"numa_binding":"true","numa_exclusive":"false"}`,
			want: map[string]string{"numa_binding": "true", "numa_exclusive": "false"}},
		{name: "invalid syntax", value: `{"numa_binding":`, want: map[string]string{}},
		{name: "type error after matching fields", value: `{"numa_binding":"true","numa_exclusive":"false","other":true}`, want: map[string]string{}},
		{name: "type error before matching fields", value: `{"other":true,"numa_binding":"true","numa_exclusive":"false"}`, want: map[string]string{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			annotations := map[string]string{}
			if tt.value != "" {
				annotations[apiconsts.PodAnnotationMemoryEnhancementKey] = tt.value
			}
			got := NewQoSConfiguration().GetQoSEnhancementKVs(nil, annotations, apiconsts.PodAnnotationMemoryEnhancementKey)
			assert.Equal(t, tt.want, got)
		})
	}
}
