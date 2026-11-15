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

package malachite

import (
	"context"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/util/errors"

	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/client"
	malachitetypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

type cgroupMetricWriter interface {
	SetCgroupMetric(string, string, utilmetric.MetricData)
	SetCgroupNumaMetric(string, int, string, utilmetric.MetricData)
}

type cgroupMetricProcessor struct {
	writer cgroupMetricWriter
}

type podMetricWriter struct{ utilmetric.PodMetricData }

func (s *podMetricWriter) SetCgroupMetric(_ string, name string, data utilmetric.MetricData) {
	s.Metrics[name] = data
}

func (s *podMetricWriter) SetCgroupNumaMetric(_ string, numa int, name string, data utilmetric.MetricData) {
	if s.NumaMetrics[numa] == nil {
		s.NumaMetrics[numa] = make(map[string]utilmetric.MetricData)
	}
	s.NumaMetrics[numa][name] = data
}

func (m *MalachiteMetricsProvisioner) updatePodMetrics(ctx context.Context) error {
	provider, ok := m.podFetcher.(types.PodMetricTargetProvider)
	if !ok {
		return nil
	}
	targets, err := provider.GetPodMetricTargets(ctx)
	if err != nil {
		return err
	}
	members := make(map[string]bool, len(targets))
	errList := make([]error, 0)
	for _, target := range targets {
		if target.Pod == nil || target.Pod.UID == "" {
			return fmt.Errorf("metric target has no UID")
		}
		uid := string(target.Pod.UID)
		if members[uid] {
			return fmt.Errorf("duplicate metric target UID %s", uid)
		}
		members[uid] = true
	}
	for _, target := range targets {
		// Resource processors share one raw response; adding a resource must
		// not add another GetCgroupStats call.
		data, err := m.collectPodMetrics(ctx, provider, target, m.preparePodMemoryMetrics, m.preparePodCPUMetrics)
		m.metricStore.SetPodMetrics(string(target.Pod.UID), data)
		if err != nil {
			errList = append(errList, err)
		}
	}
	m.metricStore.GCPodMetricSamples(members)
	return errors.NewAggregate(errList)
}

// Resource preparation captures controller identity before the shared read.
// Each returned processor writes only valid metrics and returns its own failure;
// preparation must not perform another GetCgroupStats call.
type podMetricResourcePreparer func(types.PodMetricTarget) func(*malachitetypes.MalachiteCgroupInfo, *podMetricWriter) error

func (m *MalachiteMetricsProvisioner) collectPodMetrics(ctx context.Context, provider types.PodMetricTargetProvider, target types.PodMetricTarget, resources ...podMetricResourcePreparer) (utilmetric.PodMetricData, error) {
	empty := utilmetric.PodMetricData{CgroupPath: target.CgroupPath, MappingRevision: target.MappingRevision}
	current, handled, err := provider.ResolvePodMetricTarget(ctx, target.Pod)
	if err != nil || !handled || !target.SameMapping(current) {
		return empty, fmt.Errorf("metric target %s mapping unavailable: %v", target.Pod.UID, err)
	}
	processors := make([]func(*malachitetypes.MalachiteCgroupInfo, *podMetricWriter) error, 0, len(resources))
	for _, prepare := range resources {
		processors = append(processors, prepare(target))
	}
	readCtx, cancel := context.WithTimeout(ctx, client.UpdateTimeout)
	defer cancel()
	stats, err := m.malachiteClient.GetCgroupStatsWithContext(readCtx, target.CgroupPath)
	if err != nil {
		return empty, err
	}
	sample := &podMetricWriter{PodMetricData: utilmetric.PodMetricData{
		CgroupPath: target.CgroupPath, MappingRevision: target.MappingRevision,
		Metrics: make(map[string]utilmetric.MetricData), NumaMetrics: make(map[int]map[string]utilmetric.MetricData),
	}}
	var errList []error
	for _, process := range processors {
		if err := process(stats, sample); err != nil {
			errList = append(errList, err)
		}
	}
	current, handled, err = provider.ResolvePodMetricTarget(ctx, target.Pod)
	if err != nil || !handled || !target.SameMapping(current) || ctx.Err() != nil {
		// Discard all processed metrics, but retain identity so publication
		// replaces the previous sample rather than leaving stale data readable.
		return empty, fmt.Errorf("metric target %s changed during collection: %v", target.Pod.UID, err)
	}
	return sample.PodMetricData, errors.NewAggregate(errList)
}

// Each resource captures its own controller identity before the shared read.
// Do not cache or share this check across controllers or collection rounds.
func (m *MalachiteMetricsProvisioner) prepareCgroupIdentityCheck(cgroupPath string) func() error {
	stat := m.cgroupStatFn
	if stat == nil {
		stat = os.Stat
	}
	before, prepareErr := stat(cgroupPath)
	return func() error {
		if prepareErr != nil {
			return prepareErr
		}
		after, err := stat(cgroupPath)
		if err != nil {
			return err
		}
		if !os.SameFile(before, after) {
			return fmt.Errorf("cgroup %s changed during collection", cgroupPath)
		}
		return nil
	}
}
