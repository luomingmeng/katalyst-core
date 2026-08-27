package common

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

var (
	checkCgroup2UnifiedMode   = CheckCgroup2UnifiedMode
	controllerMountInfoReader = func() (io.ReadCloser, error) {
		return os.Open("/proc/self/mountinfo")
	}
)

// ResolveControllerMount returns the mount root for subsys. On cgroup v2 all
// controllers use the unified hierarchy; on v1 it reads mountinfo so combined
// mounts such as cpu,cpuacct are resolved correctly.
func ResolveControllerMount(subsys string) (ControllerMount, error) {
	if checkCgroup2UnifiedMode() {
		return ControllerMount{Root: CgroupFSMountPoint, Unified: true}, nil
	}

	reader, err := controllerMountInfoReader()
	if err != nil {
		return ControllerMount{}, fmt.Errorf("open cgroup mountinfo: %w", err)
	}
	defer func() { _ = reader.Close() }()

	mounts, err := parseControllerMounts(reader)
	if err != nil {
		return ControllerMount{}, err
	}
	root, ok := mounts[subsys]
	if !ok {
		return ControllerMount{}, fmt.Errorf("%w: %s", ErrControllerMountUnavailable, subsys)
	}
	return ControllerMount{Root: root}, nil
}

func parseControllerMounts(reader io.Reader) (map[string]string, error) {
	mounts := make(map[string]string)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		separator := strings.Index(line, " - ")
		if separator < 0 {
			return nil, fmt.Errorf("malformed mountinfo line %q", line)
		}
		left, right := strings.Fields(line[:separator]), strings.Fields(line[separator+3:])
		if len(left) < 5 || len(right) < 3 {
			return nil, fmt.Errorf("malformed mountinfo line %q", line)
		}
		if right[0] != "cgroup" {
			continue
		}
		for _, option := range strings.Split(right[2], ",") {
			switch option {
			case CgroupSubsysCPU, CgroupSubsysCPUSet:
				mounts[option] = left[4]
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read cgroup mountinfo: %w", err)
	}
	return mounts, nil
}
