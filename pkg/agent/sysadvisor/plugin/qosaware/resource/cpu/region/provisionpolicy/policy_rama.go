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

package provisionpolicy

import (
	"fmt"
	"math"
	"strconv"

	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/helper"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const (
	metricRamaDominantIndicator       = "rama_dominant_indicator"
	metricRamaRestrictRefPolicyResult = "rama_restrict_ref_policy_result"
)

type PolicyRama struct {
	*PolicyBase
	conf        *config.Configuration
	controllers map[string]*helper.PIDController // map[metricName]controller
}

func NewPolicyRama(regionName string, regionType types.QoSRegionType, ownerPoolName string,
	conf *config.Configuration, _ interface{}, metaReader metacache.MetaReader,
	metaServer *metaserver.MetaServer, emitter metrics.MetricEmitter) ProvisionPolicy {
	p := &PolicyRama{
		conf:        conf,
		PolicyBase:  NewPolicyBase(regionName, regionType, ownerPoolName, metaReader, metaServer, emitter),
		controllers: make(map[string]*helper.PIDController),
	}

	return p
}

func (p *PolicyRama) Update() error {
	// sanity check
	if err := p.sanityCheck(); err != nil {
		return err
	}

	cpuSize := p.ControlKnobs[types.ControlKnobNonReclaimedCPUSize].Value

	// pass the current value to regulator to avoid slow start from zero after restart
	p.regulator.SetLatestCPURequirement(int(cpuSize))

	cpuSizeRaw := math.Inf(-1)
	dominantIndicator := "unknown"

	// run pid control for each indicator
	for metricName, indicator := range p.Indicators {
		params, ok := p.conf.PolicyRama.PIDParameters[metricName]
		if !ok {
			klog.Warningf("[qosaware-cpu-rama] pid parameter not found for indicator %v", metricName)
			continue
		}

		controller, ok := p.controllers[metricName]
		if !ok {
			controller = helper.NewPIDController(metricName, params)
			p.controllers[metricName] = controller
		}

		controller.SetEssentials(p.ResourceEssentials)
		cpuAdjusted := controller.Adjust(cpuSize, indicator.Target, indicator.Current)

		if cpuAdjusted > cpuSizeRaw {
			cpuSizeRaw = cpuAdjusted
			dominantIndicator = metricName
		}
	}

	period := p.conf.QoSAwarePluginConfiguration.SyncPeriod
	_ = p.emitter.StoreInt64(metricRamaDominantIndicator, int64(period.Seconds()), metrics.MetricTypeNameCount, []metrics.MetricTag{
		{Key: "metric_name", Val: dominantIndicator},
	}...)

	for metricName := range p.controllers {
		_, ok := p.conf.PolicyRama.PIDParameters[metricName]
		if !ok {
			delete(p.controllers, metricName)
		}
	}

	// restrict cpu size by reference control knob
	cpuSizeRestricted, err := p.restrictCPUSizeByReferenceControlKnob(cpuSizeRaw)
	if err != nil {
		return err
	}

	// restrict cpu size by reclaimed usage
	cpuSizeRestricted, err = p.restrictCPUSizeByReclaimedUsage(cpuSizeRestricted)
	if err != nil {
		return err
	}

	p.regulator.Regulate(cpuSizeRestricted)

	return nil
}

// restrictCPUSizeByReferenceControlKnob is to restrict cpu size by reference control knob
func (p *PolicyRama) restrictCPUSizeByReferenceControlKnob(cpuSizeRaw float64) (float64, error) {
	if p.conf.PolicyRama.RestrictedByRefPolicyName == "" {
		return cpuSizeRaw, nil
	}

	controlKnob, ok := p.ControlEssentials.ReferenceControlKnobs[p.conf.PolicyRama.RestrictedByRefPolicyName]
	if !ok {
		return 0, fmt.Errorf("get control knob of restrict reference policy %v failed", p.conf.PolicyRama.RestrictedByRefPolicyName)
	}

	nonReclaimedCPUSize, ok := controlKnob[types.ControlKnobNonReclaimedCPUSize]
	if !ok {
		return 0, fmt.Errorf("get control knob non-reclaimed-cpu-size controlKnobValue failed")
	}

	min := math.Min(nonReclaimedCPUSize.Value-p.conf.PolicyRama.RestrictedByRefPolicyMaxGap, nonReclaimedCPUSize.Value*(1-p.conf.PolicyRama.RestrictedByRefPolicyMaxGapRatio))
	max := math.Max(nonReclaimedCPUSize.Value+p.conf.PolicyRama.RestrictedByRefPolicyMaxGap, nonReclaimedCPUSize.Value*(1+p.conf.PolicyRama.RestrictedByRefPolicyMaxGapRatio))
	klog.Infof("[qosaware-cpu-rama] reference non-reclaimed cpu size %v, min: %v, max: %v", nonReclaimedCPUSize.Value, min, max)

	cpuSizeRestricted := cpuSizeRaw

	reason := ""
	if cpuSizeRaw > max {
		cpuSizeRestricted = max
		reason = "above reference control knob non-reclaimed cpu size"
	} else if cpuSizeRaw < min {
		cpuSizeRestricted = min
		reason = "below reference control knob non-reclaimed cpu size"
	}

	if cpuSizeRestricted != cpuSizeRaw {
		klog.Infof("[qosaware-cpu-rama] restrict cpu size from %.2f to %.2f, reason: %v", cpuSizeRaw, cpuSizeRestricted, reason)
	}

	_ = p.emitter.StoreFloat64(metricRamaRestrictRefPolicyResult, nonReclaimedCPUSize.Value, metrics.MetricTypeNameRaw, []metrics.MetricTag{
		{Key: "reference", Val: string(p.conf.PolicyRama.RestrictedByRefPolicyName)},
		{Key: "restricted", Val: strconv.FormatBool(cpuSizeRestricted != cpuSizeRaw)},
	}...)

	return cpuSizeRestricted, nil
}

func (p *PolicyRama) restrictCPUSizeByReclaimedUsage(cpuSizedRaw float64) (float64, error) {
	if !p.ControlEssentials.ReclaimOverlap {
		return cpuSizedRaw, nil
	}

	reclaimedUsage, reclaimedCnt := p.getReclaimStatus()
	klog.Infof("[qosaware-cpu-rama] reclaim usage %.2f #container %v", reclaimedUsage, reclaimedCnt)

	cpuSizeRestricted := cpuSizedRaw

	reason := ""
	if reclaimedCnt <= 0 {
		// do not reclaim if no reclaimed containers
		cpuSizeRestricted = p.ResourceUpperBound
		reason = "no reclaimed container"
	} else {
		// do not overlap more if reclaim usage is below threshold
		threshold := p.ResourceUpperBound - reclaimedUsage - types.ReclaimUsageMarginForOverlap
		cpuSizeRestricted = math.Max(cpuSizeRestricted, threshold)
		reason = "low reclaim usage"
	}
	if cpuSizeRestricted != cpuSizedRaw {
		klog.Infof("[qosaware-cpu-rama] restrict cpu size from %.2f to %.2f, reason: %v", cpuSizedRaw, cpuSizeRestricted, reason)
	}

	return cpuSizeRestricted, nil
}

func (p *PolicyRama) sanityCheck() error {
	var (
		isLegal bool
		errList []error
	)

	enableReclaim := p.conf.GetDynamicConfiguration().EnableReclaim

	// 1. check if enable reclaim
	if !enableReclaim {
		errList = append(errList, fmt.Errorf("reclaim disabled"))
	}

	// 2. check margin. skip update when margin is non zero
	if p.ResourceEssentials.ReservedForAllocate != 0 {
		errList = append(errList, fmt.Errorf("margin exists"))
	}

	// 3. check control knob legality
	isLegal = true
	if p.ControlKnobs == nil || len(p.ControlKnobs) <= 0 {
		isLegal = false
	} else {
		v, ok := p.ControlKnobs[types.ControlKnobNonReclaimedCPUSize]
		if !ok || v.Value <= 0 {
			isLegal = false
		}
	}
	if !isLegal {
		errList = append(errList, fmt.Errorf("illegal control knob %v", p.ControlKnobs))
	}

	// 4. check indicators legality
	if p.Indicators == nil {
		errList = append(errList, fmt.Errorf("illegal indicators"))
	}

	return errors.NewAggregate(errList)
}

func (p *PolicyRama) getReclaimStatus() (usage float64, cnt int) {
	usage = 0
	cnt = 0

	f := func(podUID string, containerName string, ci *types.ContainerInfo) bool {
		if ci.QoSLevel != apiconsts.PodAnnotationQoSLevelReclaimedCores {
			return true
		}

		containerUsage := ci.CPURequest
		m, err := p.metaServer.GetContainerMetric(podUID, containerName, consts.MetricCPUUsageContainer)
		if err == nil {
			containerUsage = m.Value
		}

		// FIXME: metric server doesn't support to report cpu usage in numa granularity,
		// so we split cpu usage evenly across the binding numas of container.
		if p.bindingNumas.Size() > 0 {
			cpuSize := 0
			for _, numaID := range p.bindingNumas.ToSliceInt() {
				cpuSize += ci.TopologyAwareAssignments[numaID].Size()
			}
			containerUsageNuma := 0.0
			cpuAssignmentCPUs := machine.CountCPUAssignmentCPUs(ci.TopologyAwareAssignments)
			if cpuAssignmentCPUs != 0 {
				containerUsageNuma = containerUsage * float64(cpuSize) / float64(cpuAssignmentCPUs)
			} else {
				// handle the case that cpuAssignmentCPUs is 0
				klog.Warningf("[qosaware-cpu-rama] cpuAssignmentCPUs is 0 for %v/%v", podUID, containerName)
				containerUsageNuma = 0
			}
			usage += containerUsageNuma
		}

		cnt += 1
		return true
	}
	p.metaReader.RangeContainer(f)

	return usage, cnt
}
