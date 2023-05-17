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

package headroom

import (
	"github.com/spf13/pflag"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/sysadvisor/qosaware/resource/memory/headroom"
)

const (
	defaultEnableBuffer                    = false
	defaultMemoryFreeEstimationUtilization = 0.6
	defaultStaticMemoryOversold            = 20 << 30 // 20GB

	defaultCacheOversoldRate                     = 0
	defaultCacheOversoldMemoryCPURatioUpperBound = 6.0
	defaultCacheOversoldMemoryCPURatioLowerBound = 3.5
)

type MemoryPolicyCanonicalOptions struct {
	EnableBuffer                          bool
	MemoryFreeEstimationUtilization       float64
	StaticMemoryOversold                  float64
	CacheOversoldRate                     float64
	CacheOversoldMemoryCPURatioUpperBound float64
	CacheOversoldMemoryCPURatioLowerBound float64
}

func NewMemoryPolicyCanonicalOptions() *MemoryPolicyCanonicalOptions {
	return &MemoryPolicyCanonicalOptions{
		EnableBuffer:                          defaultEnableBuffer,
		MemoryFreeEstimationUtilization:       defaultMemoryFreeEstimationUtilization,
		StaticMemoryOversold:                  defaultStaticMemoryOversold,
		CacheOversoldRate:                     defaultCacheOversoldRate,
		CacheOversoldMemoryCPURatioUpperBound: defaultCacheOversoldMemoryCPURatioUpperBound,
		CacheOversoldMemoryCPURatioLowerBound: defaultCacheOversoldMemoryCPURatioLowerBound,
	}
}

func (o *MemoryPolicyCanonicalOptions) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.EnableBuffer, "memory-headroom-policy-canonical-enable-buffer", o.EnableBuffer,
		"the flag to enable memory buffer")
	fs.Float64Var(&o.MemoryFreeEstimationUtilization, "memory-headroom-policy-canonical-free-estimation-utilization", o.MemoryFreeEstimationUtilization,
		"the estimation of free memory utilization, which can be used as system buffer to oversold memory")
	fs.Float64Var(&o.StaticMemoryOversold, "memory-headroom-policy-canonical-static-oversold", o.StaticMemoryOversold,
		"the static oversold memory size by bytes")
	fs.Float64Var(&o.CacheOversoldRate, "memory-headroom-policy-canonical-cache-oversold-rate", o.CacheOversoldRate,
		"the rate of cache oversold, 0 means disable cache oversold")
	fs.Float64Var(&o.CacheOversoldMemoryCPURatioUpperBound, "memory-headroom-policy-canonical-cache-oversold-memory-cpu-ratio-upper-bound", o.CacheOversoldMemoryCPURatioUpperBound,
		"the upper bound of memory to cpu ratio for enabling cache oversold")
	fs.Float64Var(&o.CacheOversoldMemoryCPURatioLowerBound, "memory-headroom-policy-canonical-cache-oversold-memory-cpu-ratio-lower-bound", o.CacheOversoldMemoryCPURatioLowerBound,
		"the lower bound of memory to cpu ratio for enabling cache oversold")
}

func (o *MemoryPolicyCanonicalOptions) ApplyTo(c *headroom.MemoryPolicyCanonicalConfiguration) error {
	c.EnableBuffer = o.EnableBuffer
	c.MemoryFreeEstimationUtilization = o.MemoryFreeEstimationUtilization
	c.StaticMemoryOversold = o.StaticMemoryOversold
	c.CacheOversoldRate = o.CacheOversoldRate
	c.CacheOversoldMemoryCPURatioUpperBound = o.CacheOversoldMemoryCPURatioUpperBound
	c.CacheOversoldMemoryCPURatioLowerBound = o.CacheOversoldMemoryCPURatioLowerBound
	return nil
}
