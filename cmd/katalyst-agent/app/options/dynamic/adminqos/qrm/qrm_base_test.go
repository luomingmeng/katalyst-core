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

package qrm

import (
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	cliflag "k8s.io/component-base/cli/flag"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/adminqos/qrm"
)

func TestNewQRMPluginOptions(t *testing.T) {
	t.Parallel()
	options := NewQRMPluginOptions()

	// Verify that all sub-options are initialized
	if options.CPUPluginOptions == nil {
		t.Errorf("CPUPluginOptions is nil")
	}
	if !options.EnableBulkheadCpusetMems {
		t.Errorf("EnableBulkheadCpusetMems default = false, want true")
	}
}

func TestQRMPluginOptions_AddFlags(t *testing.T) {
	t.Parallel()
	options := NewQRMPluginOptions()
	fss := &cliflag.NamedFlagSets{}

	options.AddFlags(fss)

	// Verify that all sub-options add flags
	cpuPluginFlagSet := fss.FlagSet("qrm-cpu-plugin")
	if cpuPluginFlagSet == nil {
		t.Errorf("qrm-cpu-plugin flag set not found")
	}
	for _, name := range []string{
		"enable-bulkhead",
		"enable-bulkhead-cpuset-topology",
		"enable-bulkhead-cpuset-mems",
		"enable-bulkhead-workqueue",
		"enable-bulkhead-system-service",
		"bind-irq-to-reclaimed-pool",
		"bulkhead-non-reclaim-pool-min-size",
		"bulkhead-default-cat-ways",
		"bulkhead-clos-cat-ways",
	} {
		if cpuPluginFlagSet.Lookup(name) == nil {
			t.Errorf("qrm-cpu-plugin flag %q not found", name)
		}
	}
}

func TestQRMPluginOptions_ApplyTo(t *testing.T) {
	t.Parallel()
	options := NewQRMPluginOptions()
	config := qrm.NewQRMPluginConfiguration()

	// Apply options to config
	err := options.ApplyTo(config)
	if err != nil {
		t.Errorf("ApplyTo failed: %v", err)
	}

	// Verify that config is updated
	if config.CPUPluginConfiguration == nil {
		t.Errorf("CPUPluginConfiguration is nil after ApplyTo")
	}
	if config.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize != 16 {
		t.Errorf("NonReclaimPoolMinSize = %d, want default 16", config.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize)
	}
	if !config.CPUPluginConfiguration.BulkheadConfig.EnableBulkheadCpusetMems {
		t.Errorf("EnableBulkheadCpusetMems = false, want default true")
	}
}

func TestQRMPluginOptions_ApplyToDynamicBulkheadConfiguration(t *testing.T) {
	t.Parallel()

	options := NewQRMPluginOptions()
	options.EnableBulkhead = true
	options.EnableBulkheadCpusetTopology = true
	options.EnableBulkheadCpusetMems = true
	options.EnableBulkheadWorkqueue = true
	options.EnableBulkheadSystemService = true
	options.BindIRQToReclaimedPool = true
	options.BulkheadNonReclaimPoolMinSize = 4
	config := qrm.NewQRMPluginConfiguration()

	err := options.ApplyTo(config)
	if err != nil {
		t.Errorf("ApplyTo failed: %v", err)
	}
	if !config.CPUPluginConfiguration.BulkheadConfig.Enable {
		t.Errorf("Enable = false, want true")
	}
	if !config.CPUPluginConfiguration.BulkheadConfig.EnableBulkheadCpusetTopology {
		t.Errorf("EnableBulkheadCpusetTopology = false, want true")
	}
	if !config.CPUPluginConfiguration.BulkheadConfig.EnableBulkheadCpusetMems {
		t.Errorf("EnableBulkheadCpusetMems = false, want true")
	}
	if !config.CPUPluginConfiguration.BulkheadConfig.EnableBulkheadWorkqueue {
		t.Errorf("EnableBulkheadWorkqueue = false, want true")
	}
	if !config.CPUPluginConfiguration.BulkheadConfig.EnableBulkheadSystemService {
		t.Errorf("EnableBulkheadSystemService = false, want true")
	}
	if !config.CPUPluginConfiguration.BindIRQToReclaimedPool {
		t.Errorf("BindIRQToReclaimedPool = false, want true")
	}
	if config.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize != 4 {
		t.Errorf("NonReclaimPoolMinSize = %d, want 4", config.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize)
	}
}

func TestQRMPluginOptions_ParseBulkheadNonReclaimPoolMinSize(t *testing.T) {
	t.Parallel()

	options := NewQRMPluginOptions()
	fss := &cliflag.NamedFlagSets{}
	options.AddFlags(fss)

	if err := fss.FlagSet("qrm-cpu-plugin").Parse([]string{"--bulkhead-non-reclaim-pool-min-size=4"}); err != nil {
		t.Fatalf("failed to parse flag: %v", err)
	}

	config := qrm.NewQRMPluginConfiguration()
	if err := options.ApplyTo(config); err != nil {
		t.Fatalf("ApplyTo failed: %v", err)
	}
	if config.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize != 4 {
		t.Fatalf("NonReclaimPoolMinSize = %d, want 4", config.CPUPluginConfiguration.BulkheadConfig.NonReclaimPoolMinSize)
	}
}

func TestQRMPluginOptions_ParseBulkheadCATWays(t *testing.T) {
	t.Parallel()

	options := NewQRMPluginOptions()
	fss := &cliflag.NamedFlagSets{}
	options.AddFlags(fss)

	if err := fss.FlagSet("qrm-cpu-plugin").Parse([]string{
		"--bulkhead-default-cat-ways=4",
		"--bulkhead-clos-cat-ways=reclaim=2,shared=3",
	}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}
	wantClosCATWays := map[string]int64{"reclaim": 2, "shared": 3}
	if !reflect.DeepEqual(options.BulkheadClosCATWays, wantClosCATWays) {
		t.Fatalf("BulkheadClosCATWays = %v, want %v", options.BulkheadClosCATWays, wantClosCATWays)
	}

	config := qrm.NewQRMPluginConfiguration()
	if err := options.ApplyTo(config); err != nil {
		t.Fatalf("ApplyTo failed: %v", err)
	}
	rdt := config.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig
	if rdt.DefaultCATWays != 4 {
		t.Fatalf("DefaultCATWays = %d, want 4", rdt.DefaultCATWays)
	}
	if !reflect.DeepEqual(rdt.ClosCATWays, wantClosCATWays) {
		t.Fatalf("ClosCATWays = %v, want %v", rdt.ClosCATWays, wantClosCATWays)
	}
	options.BulkheadClosCATWays["reclaim"] = 9
	if rdt.ClosCATWays["reclaim"] != 2 {
		t.Fatalf("ClosCATWays aliases options map: reclaim = %d, want 2", rdt.ClosCATWays["reclaim"])
	}
}

func TestQRMPluginOptions_ParseInvalidBulkheadClosCATWays(t *testing.T) {
	t.Parallel()

	options := NewQRMPluginOptions()
	fss := &cliflag.NamedFlagSets{}
	options.AddFlags(fss)

	fs := fss.FlagSet("qrm-cpu-plugin")
	fs.Init("qrm-cpu-plugin", pflag.ContinueOnError)
	err := fs.Parse(
		[]string{"--bulkhead-clos-cat-ways=reclaim=invalid"},
	)
	if err == nil {
		t.Fatal("Parse succeeded, want non-integer error")
	}
}

func TestQRMPluginOptions_ParseExplicitZeroBulkheadDefaultCATWays(t *testing.T) {
	t.Parallel()

	options := NewQRMPluginOptions()
	fss := &cliflag.NamedFlagSets{}
	options.AddFlags(fss)

	if err := fss.FlagSet("qrm-cpu-plugin").Parse([]string{"--bulkhead-default-cat-ways=0"}); err != nil {
		t.Fatalf("failed to parse flag: %v", err)
	}

	err := options.ApplyTo(qrm.NewQRMPluginConfiguration())
	if err == nil {
		t.Fatal("ApplyTo succeeded, want error")
	}
	if !strings.Contains(err.Error(), "bulkhead-default-cat-ways must be positive") {
		t.Fatalf("ApplyTo error = %q, want positive-value error", err)
	}
	if err.Error() != strings.ToLower(err.Error()) {
		t.Fatalf("ApplyTo error = %q, want lower-case error", err)
	}
}

func TestCPUPluginOptions_ParseExplicitZeroFromFirstNamedFlagSets(t *testing.T) {
	t.Parallel()

	options := NewCPUPluginOptions()
	firstFSS := &cliflag.NamedFlagSets{}
	secondFSS := &cliflag.NamedFlagSets{}
	options.AddFlags(firstFSS)
	options.AddFlags(secondFSS)

	if err := firstFSS.FlagSet("qrm-cpu-plugin").Parse([]string{"--bulkhead-default-cat-ways=0"}); err != nil {
		t.Fatalf("failed to parse flag from first NamedFlagSets: %v", err)
	}

	err := options.ApplyTo(qrm.NewCPUPluginConfiguration())
	if err == nil {
		t.Fatal("ApplyTo succeeded, want error")
	}
	if !strings.Contains(err.Error(), "bulkhead-default-cat-ways must be positive") {
		t.Fatalf("ApplyTo error = %q, want positive-value error", err)
	}
}

func TestQRMPluginOptions_ValidateBulkheadCATWays(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		defaultCATWays  int64
		closCATWays     map[string]int64
		wantErrContains string
	}{
		{name: "negative default", defaultCATWays: -1, wantErrContains: "bulkhead-default-cat-ways must be positive"},
		{name: "empty clos", closCATWays: map[string]int64{"": 2}, wantErrContains: "bulkhead-clos-cat-ways contains an empty clos"},
		{name: "zero ways", closCATWays: map[string]int64{"reclaim": 0}, wantErrContains: "bulkhead-clos-cat-ways value must be positive"},
		{name: "negative ways", closCATWays: map[string]int64{"reclaim": -1}, wantErrContains: "bulkhead-clos-cat-ways value must be positive"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			options := NewQRMPluginOptions()
			options.BulkheadDefaultCATWays = tc.defaultCATWays
			options.BulkheadClosCATWays = tc.closCATWays

			err := options.ApplyTo(qrm.NewQRMPluginConfiguration())
			if err == nil {
				t.Fatal("ApplyTo succeeded, want error")
			}
			if !strings.Contains(err.Error(), tc.wantErrContains) {
				t.Fatalf("ApplyTo error = %q, want substring %q", err, tc.wantErrContains)
			}
			if err.Error() != strings.ToLower(err.Error()) {
				t.Fatalf("ApplyTo error = %q, want lower-case error", err)
			}
		})
	}

	t.Run("zero values preserve compatibility", func(t *testing.T) {
		t.Parallel()

		options := NewQRMPluginOptions()
		config := qrm.NewQRMPluginConfiguration()
		if err := options.ApplyTo(config); err != nil {
			t.Fatalf("ApplyTo failed: %v", err)
		}
		rdt := config.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig
		if rdt.DefaultCATWays != 0 {
			t.Fatalf("DefaultCATWays = %d, want 0", rdt.DefaultCATWays)
		}
		if rdt.ClosCATWays != nil {
			t.Fatalf("ClosCATWays = %v, want nil", rdt.ClosCATWays)
		}
	})
}
