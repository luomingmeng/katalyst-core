//go:build linux

package common

import (
	"errors"
	"strings"
	"testing"
)

func TestParseControllerMounts(t *testing.T) {
	t.Parallel()

	mounts, err := parseControllerMounts(strings.NewReader(
		"29 23 0:26 / /sys/fs/cgroup/cpu,cpuacct rw,nosuid,nodev,noexec,relatime - cgroup cgroup rw,cpu,cpuacct\n" +
			"30 23 0:27 / /sys/fs/cgroup/cpuset rw,nosuid,nodev,noexec,relatime - cgroup cgroup rw,cpuset\n",
	))
	if err != nil {
		t.Fatalf("parseControllerMounts: %v", err)
	}
	if got := mounts[CgroupSubsysCPU]; got != "/sys/fs/cgroup/cpu,cpuacct" {
		t.Fatalf("cpu mount = %q", got)
	}
	if got := mounts[CgroupSubsysCPUSet]; got != "/sys/fs/cgroup/cpuset" {
		t.Fatalf("cpuset mount = %q", got)
	}
}

func TestResolveControllerMountV1CombinedCPU(t *testing.T) {
	t.Parallel()

	got, err := resolveControllerMount(CgroupSubsysCPU, false, strings.NewReader(
		"29 23 0:26 / /sys/fs/cgroup/cpu,cpuacct rw - cgroup cgroup rw,cpu,cpuacct\n",
	))
	if err != nil {
		t.Fatalf("ResolveControllerMount: %v", err)
	}
	if got.Root != "/sys/fs/cgroup/cpu,cpuacct" || got.Unified {
		t.Fatalf("mount = %+v", got)
	}
}

func TestResolveControllerMountUnavailable(t *testing.T) {
	t.Parallel()

	_, err := resolveControllerMount(CgroupSubsysCPU, false, strings.NewReader(
		"30 23 0:27 / /sys/fs/cgroup/cpuset rw - cgroup cgroup rw,cpuset\n",
	))
	if !errors.Is(err, ErrControllerMountUnavailable) {
		t.Fatalf("error = %v, want ErrControllerMountUnavailable", err)
	}
}
