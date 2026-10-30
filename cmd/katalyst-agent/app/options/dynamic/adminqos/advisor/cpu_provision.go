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

package advisor

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/errors"
	cliflag "k8s.io/component-base/cli/flag"

	"github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	workloadapi "github.com/kubewharf/katalyst-api/pkg/apis/workload/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/adminqos/advisor"
	"github.com/kubewharf/katalyst-core/pkg/consts"
)

type CPUProvisionOptions struct {
	AllowSharedCoresOverlapReclaimedCores       bool
	DisableDedicatedCoresOverlapReclaimedCores  bool
	RegionIndicatorTargetOptions                map[string]string
	IndicatorTargetGetters                      map[string]string
	IndicatorTargetDefaultGetter                string
	IndicatorTargetMetricThresholdExpandFactors map[string]string
	ReclaimedCPUMaxRatio                        float64
	FillDefaultSharePoolWithNonReclaimCPUs      bool

	// BoundedControlKnobConstraintsJSON is the raw payload of
	// --region-bounded-control-knob-constraints-json, held as a string and decoded
	// strictly in ApplyTo so malformed input fails agent startup rather than being
	// silently dropped. The decoded set is seeded straight into the dynamic
	// CPUProvisionConfiguration.ControlKnobConstraints as the startup default;
	// KCC deep-copies it each round and lets AQC override it.
	BoundedControlKnobConstraintsJSON string
}

func NewCPUProvisionOptions() *CPUProvisionOptions {
	return &CPUProvisionOptions{
		AllowSharedCoresOverlapReclaimedCores:       false,
		DisableDedicatedCoresOverlapReclaimedCores:  false,
		RegionIndicatorTargetOptions:                map[string]string{},
		IndicatorTargetGetters:                      map[string]string{},
		IndicatorTargetDefaultGetter:                string(consts.IndicatorTargetGetterSPDMin),
		IndicatorTargetMetricThresholdExpandFactors: map[string]string{},
	}
}

// ApplyTo fills up config with options
func (o *CPUProvisionOptions) ApplyTo(c *advisor.CPUProvisionConfiguration) error {
	var errList []error
	c.ReclaimedCPUMaxRatio = o.ReclaimedCPUMaxRatio
	c.FillDefaultSharePoolWithNonReclaimCPUs = o.FillDefaultSharePoolWithNonReclaimCPUs
	c.SetRequestedOverlapPolicy(
		o.AllowSharedCoresOverlapReclaimedCores,
		o.DisableDedicatedCoresOverlapReclaimedCores,
	)

	for regionType, targets := range o.RegionIndicatorTargetOptions {
		regionIndicatorTarget := make([]v1alpha1.IndicatorTargetConfiguration, 0)
		indicatorTargets := strings.Split(targets, "/")
		for _, indicatorTarget := range indicatorTargets {
			tmp := strings.Split(indicatorTarget, "=")
			if len(tmp) != 2 {
				errList = append(errList, fmt.Errorf("indicatorTarget %v is invalid", indicatorTarget))
				continue
			}
			target, err := strconv.ParseFloat(tmp[1], 64)
			if err != nil {
				errList = append(errList, err)
				continue
			}
			regionIndicatorTarget = append(regionIndicatorTarget, v1alpha1.IndicatorTargetConfiguration{Name: workloadapi.ServiceSystemIndicatorName(tmp[0]), Target: target})
		}
		c.RegionIndicatorTargetConfiguration[v1alpha1.QoSRegionType(regionType)] = regionIndicatorTarget
	}
	if o.IndicatorTargetGetters != nil && len(o.IndicatorTargetGetters) > 0 {
		c.IndicatorTargetGetters = o.IndicatorTargetGetters
	}
	if o.IndicatorTargetDefaultGetter != "" {
		c.IndicatorTargetDefaultGetter = o.IndicatorTargetDefaultGetter
	}

	if len(o.IndicatorTargetMetricThresholdExpandFactors) > 0 {
		c.IndicatorTargetMetricThresholdExpandFactors = make(map[string]float64)
		for indicatorName, expandFactor := range o.IndicatorTargetMetricThresholdExpandFactors {
			factor, err := strconv.ParseFloat(expandFactor, 64)
			if err != nil {
				errList = append(errList, err)
				continue
			}
			c.IndicatorTargetMetricThresholdExpandFactors[indicatorName] = factor
		}
	}

	// Decode the startup bounded-control-knob defaults strictly and seed the
	// dynamic CPUProvisionConfiguration.ControlKnobConstraints directly. The
	// decoded slice is a fresh object graph produced by json.Decoder, so assigning
	// it to the single field introduces no alias with flag-owned memory; the KCC
	// manager deep-copies this seeded defaultConfig on every round before Apply.
	constraints, err := parseBoundedControlKnobConstraintsJSON(o.BoundedControlKnobConstraintsJSON)
	if err != nil {
		errList = append(errList, err)
	} else {
		c.ControlKnobConstraints = constraints
	}

	return errors.NewAggregate(errList)
}

// AddFlags adds flags to the specified FlagSet.
func (o *CPUProvisionOptions) AddFlags(fss *cliflag.NamedFlagSets) {
	fs := fss.FlagSet("cpu-provision")
	fs.BoolVar(&o.AllowSharedCoresOverlapReclaimedCores, "allow-shared-cores-overlap-reclaimed-cores", o.AllowSharedCoresOverlapReclaimedCores,
		"set true to allow shared_cores overlap reclaimed_cores")
	fs.BoolVar(&o.DisableDedicatedCoresOverlapReclaimedCores, "disable-dedicated-cores-overlap-reclaimed-cores", o.DisableDedicatedCoresOverlapReclaimedCores,
		"set true to prevent dedicated_cores overlap reclaimed_cores")
	fs.StringToStringVar(&o.RegionIndicatorTargetOptions, "region-indicator-targets", o.RegionIndicatorTargetOptions,
		"indicators targets for each region, in format like cpu_sched_wait=400/cpu_iowait_ratio=0.8")
	fs.StringToStringVar(&o.IndicatorTargetGetters, "indicator-target-getters", o.IndicatorTargetGetters,
		"indicators target getters funcs keyed by indicator name, in format like cpu_usage_ratio=spd-avg/cpu_sched_wait=spd-min")
	fs.StringVar(&o.IndicatorTargetDefaultGetter, "indicator-target-default-getter", o.IndicatorTargetDefaultGetter,
		"default indicator target getter func like spd-avg")
	fs.StringToStringVar(&o.IndicatorTargetMetricThresholdExpandFactors, "indicator-target-metric-threshold-expand-factors", o.IndicatorTargetMetricThresholdExpandFactors,
		"indicator target expand factor, in format like cpu_usage_ratio=1.1,cpi=1.2")
	fs.Float64Var(&o.ReclaimedCPUMaxRatio, "reclaimed-cpu-max-ratio", o.ReclaimedCPUMaxRatio,
		"the maximum ratio of whole-machine/NUMA CPUs that can be assigned to reclaimed_cores, 0 means no limit")
	fs.BoolVar(&o.FillDefaultSharePoolWithNonReclaimCPUs, "fill-default-share-pool-with-non-reclaim-cpus", o.FillDefaultSharePoolWithNonReclaimCPUs,
		"make the default share pool consume all residual non-reclaim cpus")
	fs.StringVar(&o.BoundedControlKnobConstraintsJSON, "region-bounded-control-knob-constraints-json", o.BoundedControlKnobConstraintsJSON,
		"strict JSON array of bounded control-knob restrict defaults. Each item: {\"name\":<knob>,\"targetPolicy\":<policy>,\"referencePolicy\":<policy>,\"maxUpperGap\":<n>,\"maxLowerGap\":<n>,\"maxUpperGapRatio\":<n>,\"maxLowerGapRatio\":<n>,\"enforce\":<bool>}. targetPolicy/referencePolicy must both be set; at least one tolerance must be positive; negative tolerances are rejected.")
}

// parseBoundedControlKnobConstraintsJSON strictly decodes the flag payload into
// ControlKnobConstraints entries. It uses a streaming json.Decoder with
// DisallowUnknownFields and a second Decode that must return io.EOF, so a typo'd
// key, a malformed number, or trailing garbage after the first document fails
// startup.
func parseBoundedControlKnobConstraintsJSON(raw string) ([]v1alpha1.ControlKnobConstraints, error) {
	if strings.TrimSpace(raw) == "" {
		return []v1alpha1.ControlKnobConstraints{}, nil
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var out []v1alpha1.ControlKnobConstraints
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("decode bounded control-knob constraints: %w", err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("bounded control-knob constraints: trailing data after JSON document")
	}
	if err := validateBoundedControlKnobConstraints(out); err != nil {
		return nil, err
	}
	return out, nil
}

// validateBoundedControlKnobConstraints enforces the startup invariants: name
// non-empty; targetPolicy and referencePolicy must BOTH be set (a name-only or
// one-sided entry is rejected at startup because it carries no bounded band);
// at least one tolerance must be positive; any negative tolerance is rejected.
func validateBoundedControlKnobConstraints(in []v1alpha1.ControlKnobConstraints) error {
	for i := range in {
		c := &in[i]
		if strings.TrimSpace(string(c.Name)) == "" {
			return fmt.Errorf("bounded control-knob constraint #%d: name must be non-empty", i)
		}
		if c.TargetPolicy == "" || c.ReferencePolicy == "" {
			return fmt.Errorf("bounded control-knob constraint %q: targetPolicy and referencePolicy must both be set at startup", c.Name)
		}
		tolerances := []*float64{c.MaxUpperGap, c.MaxUpperGapRatio, c.MaxLowerGap, c.MaxLowerGapRatio}
		positive := false
		for _, g := range tolerances {
			if g == nil {
				continue
			}
			if *g < 0 {
				return fmt.Errorf("bounded control-knob constraint %q: negative tolerance %v is rejected", c.Name, *g)
			}
			if *g > 0 {
				positive = true
			}
		}
		if !positive {
			return fmt.Errorf("bounded control-knob constraint %q: at least one tolerance must be positive", c.Name)
		}
	}
	return nil
}
