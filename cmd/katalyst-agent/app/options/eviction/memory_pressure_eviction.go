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

package eviction

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/labels"
	cliflag "k8s.io/component-base/cli/flag"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/eviction"
)

// MemoryPressureEvictionOptions is the options of MemoryPressureEviction
type MemoryPressureEvictionOptions struct {
	RSSOveruseEvictionFilter     string
	SystemPressureSyncPeriod     int
	SystemPressureCoolDownPeriod int
	WorkloadPaths                []string
	MemPressureSomeThreshold     int
	MemPressureFullThreshold     int
	MemPressureDuration          int
}

// NewMemoryPressureEvictionOptions returns a new MemoryPressureEvictionOptions
func NewMemoryPressureEvictionOptions() *MemoryPressureEvictionOptions {
	return &MemoryPressureEvictionOptions{
		SystemPressureSyncPeriod: 9,
		// make sure the cool down period is greater than sync period in case it triggers many times eviction between
		// two rounds of sync
		SystemPressureCoolDownPeriod: 35,
		WorkloadPaths:                []string{"/sys/fs/cgroup/kubepods/burstable"},
		MemPressureSomeThreshold:     15,
		MemPressureFullThreshold:     4,
		MemPressureDuration:          15,
	}
}

// AddFlags parses the flags to MemoryPressureEvictionOptions
func (o *MemoryPressureEvictionOptions) AddFlags(fss *cliflag.NamedFlagSets) {
	fs := fss.FlagSet("memory-pressure-eviction")

	fs.StringVar(&o.RSSOveruseEvictionFilter, "eviction-rss-overuse-filter", o.RSSOveruseEvictionFilter,
		"the labels which used to filter pods which can be evict by rss overuse eviction")
	fs.IntVar(&o.SystemPressureSyncPeriod, "eviction-system-pressure-sync-period", o.SystemPressureSyncPeriod,
		"system pressure plugin detection interval")
	fs.IntVar(&o.SystemPressureCoolDownPeriod, "eviction-system-pressure-cool-down-period", o.SystemPressureCoolDownPeriod,
		"the cool down time between system pressure plugin executes every two eviction")
	fs.StringSliceVar(&o.WorkloadPaths, "workload-paths", o.WorkloadPaths,
		"non-overlapping absolute workload cgroup roots for memory PSI")
	fs.IntVar(&o.MemPressureSomeThreshold, "eviction-system-pressure-some-threshold", o.MemPressureSomeThreshold,
		"the vaule of the threshold of memory.pressure.some")
	fs.IntVar(&o.MemPressureFullThreshold, "eviction-system-pressure-full-threshold", o.MemPressureFullThreshold,
		"the vaule of the threshold of memory.pressure.full")
	fs.IntVar(&o.MemPressureDuration, "eviction-system-pressure-duration", o.MemPressureDuration,
		"the vaule of the default duration set for period of high memory pressure")
}

// ApplyTo applies MemoryPressureEvictionOptions to MemoryPressureEvictionConfiguration
func (o *MemoryPressureEvictionOptions) ApplyTo(c *eviction.MemoryPressureEvictionConfiguration) error {
	if len(o.WorkloadPaths) == 0 {
		return fmt.Errorf("memory PSI requires at least one workload cgroup root")
	}
	seen := make(map[string]bool, len(o.WorkloadPaths))
	roots := make([]string, 0, len(o.WorkloadPaths))
	for _, value := range o.WorkloadPaths {
		value = strings.TrimSpace(value)
		if value == "" || !filepath.IsAbs(value) {
			return fmt.Errorf("PSI workload root must be an absolute nonempty path: %q", value)
		}
		for _, component := range strings.Split(value, string(filepath.Separator)) {
			if component == ".." {
				return fmt.Errorf("PSI workload root contains traversal: %q", value)
			}
		}
		clean := filepath.Clean(value)
		if !seen[clean] {
			seen[clean] = true
			roots = append(roots, clean)
		}
	}
	sort.Strings(roots)
	for i, root := range roots {
		for _, other := range roots[i+1:] {
			rel, err := filepath.Rel(root, other)
			if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return fmt.Errorf("PSI workload roots overlap: %s and %s", root, other)
			}
		}
	}
	if o.RSSOveruseEvictionFilter != "" {
		labelFilter, err := labels.ConvertSelectorToLabelsMap(o.RSSOveruseEvictionFilter)
		if err != nil {
			return fmt.Errorf("parse \"eviction-rss-overuse-filter\" flag failed, value:%v, err: %v", o.RSSOveruseEvictionFilter, err)
		}
		c.RSSOveruseEvictionFilter = labelFilter
	}
	c.SystemPressureSyncPeriod = o.SystemPressureSyncPeriod
	c.SystemPressureCoolDownPeriod = o.SystemPressureCoolDownPeriod
	c.WorkloadPaths = roots
	c.MemPressureSomeThreshold = o.MemPressureSomeThreshold
	c.MemPressureFullThreshold = o.MemPressureFullThreshold
	c.MemPressureDuration = o.MemPressureDuration
	return nil
}
