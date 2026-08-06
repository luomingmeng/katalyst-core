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

package api

import (
	"context"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
)

type HandlerContext struct {
	cpusetutil.CPUSetAdjustmentHandlerCtx
	// View is an owned copy of the partition that the current plugin may read
	// and mutate without aliasing manager state. Before topology convergence it
	// contains the desired partition; dependent plugins receive a view rebuilt
	// from the topology layer's write-verified reclaim target.
	View                 *model.CPUSetPartitionView
	DesiredView          *model.DesiredView
	AppliedView          *model.AppliedView
	AppliedViewRevision  uint64
	ReportTopologyResult func(TopologyResult)
}

// TopologyResult is the typed handoff from the cpuset topology owner to the
// manager. AppliedView is publishable only when convergence was verified
// against the coordinator's still-current final snapshot.
type TopologyResult struct {
	Attempted            int
	Applied              int
	Skipped              int
	Failed               int
	Deferred             int
	Converged            bool
	ParentSafe           bool
	LeafDeferred         bool
	DeferredLeafCount    int
	DeferredCPUCount     int
	FinalSnapshotCurrent bool
	ConvergenceReport    topology.ConvergenceReport
	AppliedView          *model.AppliedView
}

// DAGApplyResult is the Bulkhead-layer result returned by the topology owner.
// AppliedView is valid only when (FullyConverged or ParentSafe) and
// FinalSnapshotCurrent are both true. It is derived directly from the
// coordinator's final snapshot and is the canonical handoff to the manager.
type DAGApplyResult struct {
	Attempted            int
	Applied              int
	Skipped              int
	Failed               int
	Deferred             int
	FullyConverged       bool
	ParentSafe           bool
	DeferredLeafCount    int
	DeferredCPUCount     int
	FinalSnapshotCurrent bool
	ConvergenceReport    topology.ConvergenceReport
	AppliedView          *model.AppliedView
}

type PeriodicalHandlerContext struct {
	CoreConf            *config.Configuration
	ExtraConf           interface{}
	DynamicConf         *dynamicconfig.Configuration
	Emitter             metrics.MetricEmitter
	MetaServer          *metaserver.MetaServer
	AppliedView         *model.AppliedView
	AppliedViewRevision uint64
	// AppliedViewValidForPeriodical is true only when the manager published
	// AppliedView in the latest CPUSetAdjustment handler round. A non-nil
	// AppliedView without this flag must be treated as stale internal state and
	// must not authorize periodical side effects.
	AppliedViewValidForPeriodical bool
	// EffectiveEnabled is derived from the same state-aware rule used by
	// CPUSetAdjustmentHandler. nil distinguishes an unset value from an
	// explicit disabled state.
	EffectiveEnabled *bool
}

type Plugin interface {
	Name() string
	Enable(HandlerContext) bool
	CPUSetAdjustmentHandler(context.Context, HandlerContext) error
	CPUSetAdjustmentDisabledHandler(context.Context, HandlerContext) error
	PeriodicalHandler(context.Context, PeriodicalHandlerContext) error
}

type TopologyPlugin interface {
	Plugin
	Apply(context.Context, HandlerContext) (DAGApplyResult, error)
}

type PluginFactory func(conf *config.Configuration) Plugin
