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

package registry

import (
	"reflect"
	"testing"
)

func TestNewDefaultPluginsPreservesOrder(t *testing.T) {
	t.Parallel()

	plugins, err := NewDefaultPlugins(nil)
	if err != nil {
		t.Fatalf("NewDefaultPlugins failed: %v", err)
	}
	got := make([]string, 0, len(plugins))
	cpuMetricsCount := 0
	for _, plugin := range plugins {
		name := plugin.Name()
		got = append(got, name)
		if name == "cpu_metrics" {
			cpuMetricsCount++
		}
	}
	want := []string{"cpuset_topology", "cpuset_mems", "workqueue", "system_service", "rdt_cpulist", "rdt_cat", "cpu_metrics"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected plugin order, got %v want %v", got, want)
	}
	if cpuMetricsCount != 1 {
		t.Fatalf("cpu_metrics registration count = %d, want exactly 1", cpuMetricsCount)
	}
}
