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

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetExistingRelativeCgroupPaths(t *testing.T) {
	t.Parallel()

	checkedPaths := make([]string, 0)
	pathExists := func(path string) bool {
		checkedPaths = append(checkedPaths, path)
		return path == GetAbsCgroupPath(DefaultSelectedSubsys, "/existing")
	}

	got := getExistingRelativeCgroupPathsForSubsys(pathExists, DefaultSelectedSubsys, "/existing", "", "/missing")

	require.Equal(t, []string{"/existing"}, got)
	require.Equal(t, []string{
		GetAbsCgroupPath(DefaultSelectedSubsys, "/existing"),
		GetAbsCgroupPath(DefaultSelectedSubsys, "/missing"),
	}, checkedPaths)
	require.Nil(t, getExistingRelativeCgroupPathsForSubsys(pathExists, DefaultSelectedSubsys))
}

func TestGetExistingRelativeCgroupPathsForSubsys(t *testing.T) {
	t.Parallel()

	checkedPaths := make([]string, 0)
	pathExists := func(path string) bool {
		checkedPaths = append(checkedPaths, path)
		return path == GetAbsCgroupPath(CgroupSubsysMemory, "/existing")
	}

	got := getExistingRelativeCgroupPathsForSubsys(
		pathExists,
		CgroupSubsysMemory,
		"/missing",
		"/existing",
	)

	require.Equal(t, []string{"/existing"}, got)
	require.Equal(t, []string{
		GetAbsCgroupPath(CgroupSubsysMemory, "/missing"),
		GetAbsCgroupPath(CgroupSubsysMemory, "/existing"),
	}, checkedPaths)
}
