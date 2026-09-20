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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
)

func TestPodRelativeCgroupPathCandidatesUseConfiguredRootsWithoutExistenceLookup(t *testing.T) {
	t.Parallel()

	got := podRelativeCgroupPathCandidates([]string{
		"/kubepods",
		"/kubepods/burstable",
		"/kubepods.slice",
		"/kubepods.slice/kubepods-burstable.slice",
		"/kubepods.slice/kubepods-offline.slice",
		"/kubepods/burstable",
	}, "abc-def")
	want := []string{
		"/kubepods/podabc-def",
		"/kubepods/burstable/podabc-def",
		"/kubepods.slice/kubepods-podabc_def.slice",
		"/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-podabc_def.slice",
		"/kubepods.slice/kubepods-offline.slice/kubepods-offline-podabc_def.slice",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pod candidates = %v, want %v", got, want)
	}
}

func TestPodRelativeCgroupPathCandidatesForQOSSelectsCanonicalConfiguredRoot(t *testing.T) {
	t.Parallel()

	roots := []string{
		"/kubepods",
		"/kubepods/burstable",
		"/kubepods/besteffort",
		"/kubepods/offline-besteffort",
		"/kubepods.slice",
		"/kubepods.slice/kubepods-burstable.slice",
		"/kubepods.slice/kubepods-besteffort.slice",
	}
	tests := []struct {
		name string
		qos  v1.PodQOSClass
		want []string
	}{
		{
			name: "guaranteed",
			qos:  v1.PodQOSGuaranteed,
			want: []string{
				"/kubepods/podabc-def",
				"/kubepods.slice/kubepods-podabc_def.slice",
			},
		},
		{
			name: "burstable",
			qos:  v1.PodQOSBurstable,
			want: []string{
				"/kubepods/burstable/podabc-def",
				"/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-podabc_def.slice",
			},
		},
		{
			name: "best effort",
			qos:  v1.PodQOSBestEffort,
			want: []string{
				"/kubepods/besteffort/podabc-def",
				"/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-podabc_def.slice",
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := podRelativeCgroupPathCandidatesForQOS(roots, "abc-def", tc.qos)
			require.Equal(t, tc.want, got)
		})
	}
}

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
