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

package manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
)

func TestSetExtraCGMemLimitWithTimeoutAndAbsCGPathAllowsZeroOnCgroupV1(t *testing.T) {
	defer mockey.UnPatchAll()
	mockey.Mock(common.CheckCgroup2UnifiedMode).Return(false).Build()

	cgroupDir := t.TempDir()
	limitFile := filepath.Join(cgroupDir, "memory.limit_in_bytes")
	require.NoError(t, os.WriteFile(limitFile, []byte("1024\n"), 0o644))

	err := SetExtraCGMemLimitWithTimeoutAndAbsCGPath(1, cgroupDir, 0)

	require.NoError(t, err)
	got, err := os.ReadFile(limitFile)
	require.NoError(t, err)
	require.Equal(t, "0\n", string(got))
}
