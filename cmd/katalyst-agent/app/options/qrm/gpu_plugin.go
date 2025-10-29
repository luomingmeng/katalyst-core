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
	"k8s.io/apimachinery/pkg/api/resource"
	cliflag "k8s.io/component-base/cli/flag"

	"github.com/kubewharf/katalyst-core/cmd/katalyst-agent/app/options/qrm/gpustrategy"
	qrmconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm"
)

type GPUOptions struct {
	PolicyName                 string
	GPUDeviceNames             []string
	GPUMemoryAllocatablePerGPU string
	SkipGPUStateCorruption     bool
	RDMADeviceNames            []string

	GPUStrategyOptions *gpustrategy.GPUStrategyOptions
}

func NewGPUOptions() *GPUOptions {
	return &GPUOptions{
		PolicyName:                 "static",
		GPUDeviceNames:             []string{"nvidia.com/gpu"},
		GPUMemoryAllocatablePerGPU: "100",
		RDMADeviceNames:            []string{},
		GPUStrategyOptions:         gpustrategy.NewGPUStrategyOptions(),
	}
}

func (o *GPUOptions) AddFlags(fss *cliflag.NamedFlagSets) {
	fs := fss.FlagSet("gpu_resource_plugin")

	fs.StringVar(&o.PolicyName, "gpu-resource-plugin-policy",
		o.PolicyName, "The policy gpu resource plugin should use")
	fs.StringSliceVar(&o.GPUDeviceNames, "gpu-resource-names", o.GPUDeviceNames, "The name of the GPU resource")
	fs.StringVar(&o.GPUMemoryAllocatablePerGPU, "gpu-memory-allocatable-per-gpu",
		o.GPUMemoryAllocatablePerGPU, "The total memory allocatable for each GPU, e.g. 100")
	fs.BoolVar(&o.SkipGPUStateCorruption, "skip-gpu-state-corruption",
		o.SkipGPUStateCorruption, "skip gpu state corruption, and it will be used after updating state properties")
	fs.StringSliceVar(&o.RDMADeviceNames, "rdma-resource-names", o.RDMADeviceNames, "The name of the RDMA resource")
	o.GPUStrategyOptions.AddFlags(fss)
}

func (o *GPUOptions) ApplyTo(conf *qrmconfig.GPUQRMPluginConfig) error {
	conf.PolicyName = o.PolicyName
	conf.GPUDeviceNames = o.GPUDeviceNames
	gpuMemory, err := resource.ParseQuantity(o.GPUMemoryAllocatablePerGPU)
	if err != nil {
		return err
	}
	conf.GPUMemoryAllocatablePerGPU = gpuMemory
	conf.SkipGPUStateCorruption = o.SkipGPUStateCorruption
	conf.RDMADeviceNames = o.RDMADeviceNames
	if err := o.GPUStrategyOptions.ApplyTo(conf.GPUStrategyConfig); err != nil {
		return err
	}
	return nil
}
