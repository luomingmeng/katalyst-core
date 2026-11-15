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

package types

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

// PodMetricTarget identifies a Pod projection whose metrics describe the whole
// workload rather than individual containers. MappingRevision changes when its mapping
// changes or the UID disappears and returns; an unchanged refresh retains it.
type PodMetricTarget struct {
	// List results are caller-owned snapshots. Resolve may reuse its input Pod
	// after checking the mapping, but must never expose the inventory's Pod.
	// Callers must not mutate a Pod while collecting or reading its metrics.
	Pod             *v1.Pod
	CgroupPath      string
	MappingRevision uint64
}

// SameMapping compares the binding used by both collection and metric reads.
// It does not establish source availability or normalize paths; those remain
// the provider's responsibility.
func (t PodMetricTarget) SameMapping(other PodMetricTarget) bool {
	if t.Pod == nil || other.Pod == nil || t.CgroupPath == "" {
		return false
	}
	if t.CgroupPath != other.CgroupPath || t.MappingRevision != other.MappingRevision {
		return false
	}
	if t.Pod.UID != other.Pod.UID || t.Pod.Name != other.Pod.Name || t.Pod.Namespace != other.Pod.Namespace {
		return false
	}
	return ptr.Equal(t.Pod.Spec.RuntimeClassName, other.Pod.Spec.RuntimeClassName)
}

// PodMetricTargetProvider is optional. A successful list must be complete for
// this provider's source, never a mixed-source fallback. Resolution must remain
// source-bound even when a previously returned target is no longer available.
type PodMetricTargetProvider interface {
	GetPodMetricTargets(ctx context.Context) ([]PodMetricTarget, error)
	ResolvePodMetricTarget(ctx context.Context, pod *v1.Pod) (PodMetricTarget, bool, error)
}
