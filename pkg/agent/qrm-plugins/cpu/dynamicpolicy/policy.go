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

package dynamicpolicy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"
	maputil "k8s.io/kubernetes/pkg/util/maps"
	"k8s.io/utils/clock"

	"github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-api/pkg/plugins/skeleton"
	"github.com/kubewharf/katalyst-core/cmd/katalyst-agent/app/agent"
	"github.com/kubewharf/katalyst-core/cmd/katalyst-agent/app/agent/qrm"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/accompanyresource"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/advisorsvc"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	cpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/calculator"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpueviction"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer/policy"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer/registry"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/irqtuner"
	irqtuingcontroller "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/irqtuner/controller"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/validator"
	cpuutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/util"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/agent/utilcomponent/featuregatenegotiation"
	"github.com/kubewharf/katalyst-core/pkg/agent/utilcomponent/periodicalhandler"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
	"github.com/kubewharf/katalyst-core/pkg/config/generic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	podmeta "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/resourcepackage"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/kubewharf/katalyst-core/pkg/util/metric"
	"github.com/kubewharf/katalyst-core/pkg/util/process"
	"github.com/kubewharf/katalyst-core/pkg/util/reclaim"
	"github.com/kubewharf/katalyst-core/pkg/util/timemonitor"
)

type allocationPodMetaContextKey struct{}

func podMetaFromResourceRequest(req *pluginapi.ResourceRequest) metav1.ObjectMeta {
	podMeta := (&metav1.ObjectMeta{
		UID:         types.UID(req.PodUid),
		Namespace:   req.PodNamespace,
		Name:        req.PodName,
		Labels:      req.Labels,
		Annotations: req.Annotations,
	}).DeepCopy()
	return *podMeta
}

func withAllocationPodMeta(ctx context.Context, req *pluginapi.ResourceRequest) context.Context {
	return context.WithValue(ctx, allocationPodMetaContextKey{}, podMetaFromResourceRequest(req))
}

func allocationPodMetaFromContext(ctx context.Context) (metav1.ObjectMeta, bool) {
	podMeta, ok := ctx.Value(allocationPodMetaContextKey{}).(metav1.ObjectMeta)
	return podMeta, ok
}

const (
	cpuPluginStateFileName = "cpu_plugin_state"

	reservedReclaimedCPUsSize = 4

	cpusetCheckPeriod             = 10 * time.Second
	stateCheckPeriod              = 30 * time.Second
	maxResidualTime               = 5 * time.Minute
	syncCPUIdlePeriod             = 30 * time.Second
	syncCPUBurstPeriod            = 10 * time.Second
	syncSystemExclusivePoolPeriod = 10 * time.Second
	syncCPUWeightPeriod           = 10 * time.Second
	syncBulkheadPeriod            = 30 * time.Second
	rampUpTransitionPeriod        = 30 * time.Second

	healthCheckTolerationTimes = 3
)

var (
	AccompanyResourceRegistry = accompanyresource.NewRegistry()
	packAllocationResponse    = cpuutil.PackAllocationResponse
)

func rampUpDeadlineReached(initTime time.Time, transitionPeriod time.Duration, now time.Time) bool {
	return !now.Before(initTime.Add(transitionPeriod))
}

func shouldAllocationFinishRampUp(
	allocationInfo *state.AllocationInfo,
	transitionPeriod time.Duration,
	now time.Time,
) (bool, error) {
	if allocationInfo == nil || !allocationInfo.RampUp {
		return false, nil
	}
	initTime, err := time.Parse(util.QRMTimeFormat, allocationInfo.InitTimestamp)
	if err != nil {
		return false, err
	}
	return rampUpDeadlineReached(initTime, transitionPeriod, now), nil
}

// AllocationHook is a hook function which can be registered and called when allocationInfo changes.
// It is designed to intercept state updates and perform actions like injecting or updating annotations
// (e.g., NUMA topology information) based on the differences between old and new allocation info.
type AllocationHook func(oldAllocationInfo, newAllocationInfo *state.AllocationInfo) error

type allocationRequestKey struct {
	podUID        string
	containerName string
}

type allocationRequestLock struct {
	sync.Mutex
	refCount int
}

type allocationRollbackSnapshot struct {
	revision                uint64
	podEntries              state.PodEntries
	machineState            state.NUMANodeMap
	allowOverlap            bool
	disableDedicatedOverlap bool
}

// DynamicPolicy is the policy that's used by default;
// it will consider the dynamic running information to calculate
// and adjust resource requirements and configurations
type DynamicPolicy struct {
	sync.RWMutex
	pluginapi.UnimplementedResourcePluginServer

	name    string
	stopCh  chan struct{}
	started bool

	emitter     metrics.MetricEmitter
	metaServer  *metaserver.MetaServer
	machineInfo *machine.KatalystMachineInfo

	advisorClient    advisorapi.CPUAdvisorClient
	advisorConn      *grpc.ClientConn
	advisorValidator *validator.CPUAdvisorValidator
	advisorapi.UnimplementedCPUPluginServer
	advisorMonitor     *timemonitor.TimeMonitor
	featureGateManager featuregatenegotiation.FeatureGateManager

	state                          state.State
	residualHitMap                 map[string]int64
	allocationHandlers             map[string]util.AllocationHandler
	hintHandlers                   map[string]util.HintHandler
	allocationHooks                []AllocationHook
	allocationRequestLocksMu       sync.Mutex
	allocationRequestLocks         map[allocationRequestKey]*allocationRequestLock
	cpuSetAdjustmentHandlers       map[string]cpusetutil.CPUSetAdjustmentHandler
	cpuSetAdjustmentExecution      chan struct{}
	cpuSetAdjustmentRetryMu        sync.Mutex
	cpuSetAdjustmentRetryQueued    bool
	cpuSetAdjustmentRetryAgain     bool
	cpuSetAdjustmentRetryDirty     bool
	cpuSetAdjustmentRetryReasons   map[cpusetutil.CPUSetAdjustmentRetryReason]struct{}
	cpuSetAdjustmentRetryStopCh    <-chan struct{}
	cpuSetAdjustmentRetryStopping  bool
	cpuSetAdjustmentRetryWG        sync.WaitGroup
	advisorPostCommitTarget        *advisorPostCommitTarget
	advisorPostCommitCheckpointDir string
	cpuSetAdjustmentGeneration     uint64

	cpuPressureEviction       agent.Component
	cpuPressureEvictionCancel context.CancelFunc

	resourcePackageManager *resourcepackage.CachedResourcePackageManager

	irqTuner        irqtuner.Tuner
	bulkheadManager *bulkhead.Manager

	// those are parsed from configurations
	// todo if we want to use dynamic configuration, we'd better not use self-defined conf
	enableCPUAdvisor                          bool
	getAdviceInterval                         time.Duration
	reservedCPUs                              machine.CPUSet
	cpuAdvisorSocketAbsPath                   string
	cpuPluginSocketAbsPath                    string
	extraStateFileAbsPath                     string
	enableReclaimNUMABinding                  bool
	enableSNBHighNumaPreference               bool
	enableCPUIdle                             bool
	enableSyncingCPUIdle                      bool
	enableCPUBurst                            bool
	reclaimRelativeRootCgroupPaths            []string
	numaBindingReclaimRelativeRootCgroupPaths map[int][]string
	qosConfig                                 *generic.QoSConfiguration
	dynamicConfig                             *dynamicconfig.DynamicAgentConfiguration
	conf                                      *config.Configuration
	podDebugAnnoKeys                          []string
	podAnnotationKeptKeys                     []string
	podLabelKeptKeys                          []string
	numaBindingResultAnnotationKey            string
	numaNumberAnnotationKey                   string
	numaIDsAnnotationKey                      string
	topologyAllocationAnnotationKey           string
	transitionPeriod                          time.Duration

	reservedReclaimedCPUsSize                 int
	reservedReclaimedCPUSet                   machine.CPUSet
	reservedReclaimedTopologyAwareAssignments map[int]machine.CPUSet

	sharedCoresNUMABindingHintOptimizer    hintoptimizer.HintOptimizer
	dedicatedCoresNUMABindingHintOptimizer hintoptimizer.HintOptimizer

	reclaimConsumersForKCNR []string
}

func (p *DynamicPolicy) lockAllocationRequest(podUID, containerName string) func() {
	key := allocationRequestKey{podUID: podUID, containerName: containerName}

	p.allocationRequestLocksMu.Lock()
	if p.allocationRequestLocks == nil {
		p.allocationRequestLocks = make(map[allocationRequestKey]*allocationRequestLock)
	}
	requestLock := p.allocationRequestLocks[key]
	if requestLock == nil {
		requestLock = &allocationRequestLock{}
		p.allocationRequestLocks[key] = requestLock
	}
	requestLock.refCount++
	p.allocationRequestLocksMu.Unlock()

	requestLock.Lock()
	return func() {
		requestLock.Unlock()

		p.allocationRequestLocksMu.Lock()
		requestLock.refCount--
		if requestLock.refCount == 0 && p.allocationRequestLocks[key] == requestLock {
			delete(p.allocationRequestLocks, key)
		}
		p.allocationRequestLocksMu.Unlock()
	}
}

func (p *DynamicPolicy) rollbackAllocationState(
	req *pluginapi.ResourceRequest,
	snapshot allocationRollbackSnapshot,
) error {
	if p.state.GetRevision() == snapshot.revision {
		return nil
	}

	err := p.state.CommitAdvisorStateIfRevision(
		snapshot.revision,
		snapshot.podEntries,
		snapshot.machineState,
		snapshot.allowOverlap,
		snapshot.disableDedicatedOverlap,
		false,
	)
	if err == nil {
		return nil
	}
	if !errors.Is(err, state.ErrStaleStateRevision) {
		return fmt.Errorf("restore allocation state: %w", err)
	}

	currentRevision := p.state.GetRevision()
	currentPodEntries := p.state.GetPodEntries()
	rollbackAllocationInfo := snapshot.podEntries[req.PodUid][req.ContainerName]
	if rollbackAllocationInfo != nil {
		if currentPodEntries[req.PodUid] == nil {
			currentPodEntries[req.PodUid] = make(state.ContainerEntries)
		}
		currentPodEntries[req.PodUid][req.ContainerName] = rollbackAllocationInfo.Clone()
	} else if currentPodEntries[req.PodUid] != nil {
		delete(currentPodEntries[req.PodUid], req.ContainerName)
		if len(currentPodEntries[req.PodUid]) == 0 {
			delete(currentPodEntries, req.PodUid)
		}
	}

	currentMachineState, err := generateMachineStateFromPodEntries(
		p.machineInfo.CPUTopology,
		currentPodEntries,
		p.state.GetMachineState(),
	)
	if err != nil {
		return fmt.Errorf("generate machine state for stale allocation rollback: %w", err)
	}

	allowOverlap := p.state.GetAllowSharedCoresOverlapReclaimedCores()
	disableDedicatedOverlap := p.state.GetDisableDedicatedCoresOverlapReclaimedCores()
	planningState := state.NewTransientState(p.machineInfo.CPUTopology)
	if err := planningState.CommitAdvisorState(
		currentPodEntries,
		currentMachineState,
		allowOverlap,
		disableDedicatedOverlap,
		false,
	); err != nil {
		return fmt.Errorf("initialize stale allocation rollback planning state: %w", err)
	}

	planningPolicy := p.newRampUpPlanningPolicy(planningState)
	if err := planningPolicy.adjustAllocationEntriesWithRampUpFloor(
		currentPodEntries,
		currentMachineState,
		false,
		machine.NewCPUSet(),
		false,
	); err != nil {
		return fmt.Errorf("replan pools for stale allocation rollback: %w", err)
	}

	if err := p.state.CommitAdvisorStateIfRevision(
		currentRevision,
		planningState.GetPodEntries(),
		planningState.GetMachineState(),
		allowOverlap,
		disableDedicatedOverlap,
		false,
	); err != nil {
		return fmt.Errorf("commit stale allocation rollback: %w", err)
	}
	return nil
}

func NewDynamicPolicy(agentCtx *agent.GenericContext, conf *config.Configuration,
	_ interface{}, agentName string,
) (bool, agent.Component, error) {
	// add watcher for general gvrs needed in most cases
	reservedCPUs, reserveErr := cpuutil.GetCoresReservedForSystem(conf, agentCtx.MetaServer, agentCtx.KatalystMachineInfo, agentCtx.CPUDetails.CPUs().Clone())
	if reserveErr != nil {
		return false, agent.ComponentStub{}, fmt.Errorf("GetCoresReservedForSystem for reservedCPUsNum: %d, reservedCPUList: %s failed with error: %v",
			conf.ReservedCPUCores, conf.ReservedCPUList, reserveErr)
	}

	wrappedEmitter := agentCtx.EmitterPool.GetDefaultMetricsEmitter().WithTags(agentName, metrics.MetricTag{
		Key: util.QRMPluginPolicyTagName,
		Val: cpuconsts.CPUResourcePluginPolicyNameDynamic,
	})

	stateImpl, stateErr := state.NewCheckpointState(conf.StateDirectoryConfiguration, cpuPluginStateFileName,
		cpuconsts.CPUResourcePluginPolicyNameDynamic, agentCtx.CPUTopology, conf.SkipCPUStateCorruption, state.GenerateMachineStateFromPodEntries, wrappedEmitter)
	if stateErr != nil {
		return false, agent.ComponentStub{}, fmt.Errorf("NewCheckpointState failed with error: %v", stateErr)
	}

	state.SetReadonlyState(stateImpl)
	state.SetReadWriteState(stateImpl)

	var (
		cpuPressureEviction agent.Component
		err                 error
	)
	if conf.EnableCPUPressureEviction {
		cpuPressureEviction, err = cpueviction.NewCPUPressureEviction(
			agentCtx.EmitterPool.GetDefaultMetricsEmitter(), agentCtx.MetaServer, conf, stateImpl)
		if err != nil {
			return false, agent.ComponentStub{}, err
		}
	}
	bulkheadManager, err := bulkhead.NewManager(conf)
	if err != nil {
		return false, agent.ComponentStub{}, fmt.Errorf("dynamic policy init bulkhead manager failed with error: %v", err)
	}

	// since the reservedCPUs won't influence stateImpl directly.
	// so we don't modify stateImpl with reservedCPUs here.
	// for those pods have already been allocated reservedCPUs,
	// we won't touch them and wait them to be deleted the next update.
	policyImplement := &DynamicPolicy{
		name:   fmt.Sprintf("%s_%s", agentName, cpuconsts.CPUResourcePluginPolicyNameDynamic),
		stopCh: make(chan struct{}),

		machineInfo: agentCtx.KatalystMachineInfo,
		emitter:     wrappedEmitter,
		metaServer:  agentCtx.MetaServer,

		resourcePackageManager: resourcepackage.NewCachedResourcePackageManager(agentCtx.MetaServer.ResourcePackageManager),

		state:          stateImpl,
		residualHitMap: make(map[string]int64),

		advisorValidator:   validator.NewCPUAdvisorValidator(stateImpl, agentCtx.KatalystMachineInfo),
		featureGateManager: featuregatenegotiation.NewFeatureGateManager(conf),

		cpuPressureEviction: cpuPressureEviction,
		bulkheadManager:     bulkheadManager,

		conf:                           conf,
		qosConfig:                      conf.QoSConfiguration,
		dynamicConfig:                  conf.DynamicAgentConfiguration,
		cpuAdvisorSocketAbsPath:        conf.CPUAdvisorSocketAbsPath,
		cpuPluginSocketAbsPath:         conf.CPUPluginSocketAbsPath,
		enableReclaimNUMABinding:       conf.EnableReclaimNUMABinding,
		enableSNBHighNumaPreference:    conf.EnableSNBHighNumaPreference,
		enableCPUAdvisor:               conf.CPUQRMPluginConfig.EnableCPUAdvisor,
		getAdviceInterval:              conf.CPUQRMPluginConfig.GetAdviceInterval,
		reservedCPUs:                   reservedCPUs,
		extraStateFileAbsPath:          conf.ExtraStateFileAbsPath,
		enableCPUBurst:                 conf.CPUQRMPluginConfig.EnableCPUBurst,
		enableSyncingCPUIdle:           conf.CPUQRMPluginConfig.EnableSyncingCPUIdle,
		enableCPUIdle:                  conf.CPUQRMPluginConfig.EnableCPUIdle,
		reclaimRelativeRootCgroupPaths: reclaim.AggregateCgroupPaths(),
		numaBindingReclaimRelativeRootCgroupPaths: reclaim.AggregateNumaBindingCgroupPaths(),
		podDebugAnnoKeys:                conf.PodDebugAnnoKeys,
		podAnnotationKeptKeys:           conf.PodAnnotationKeptKeys,
		podLabelKeptKeys:                conf.PodLabelKeptKeys,
		numaBindingResultAnnotationKey:  conf.NUMABindingResultAnnotationKey,
		numaNumberAnnotationKey:         conf.NUMANumberAnnotationKey,
		numaIDsAnnotationKey:            conf.NUMAIDsAnnotationKey,
		topologyAllocationAnnotationKey: conf.TopologyAllocationAnnotationKey,
		transitionPeriod:                rampUpTransitionPeriod,
		reservedReclaimedCPUsSize:       general.Max(reservedReclaimedCPUsSize, agentCtx.KatalystMachineInfo.NumNUMANodes),
		reclaimConsumersForKCNR:         conf.ReclaimConsumersForKCNR,
	}
	policyImplement.advisorPostCommitCheckpointDir, _ = conf.StateDirectoryConfiguration.GetCurrentAndPreviousStateFileDirectory()

	policyImplement.RegisterAllocationHook(policyImplement.topologyAllocationHook)

	// initialize hint optimizer
	err = policyImplement.initHintOptimizers()
	if err != nil {
		return false, nil, err
	}

	if conf.EnableIRQTuner {
		irqTuner, err := irqtuingcontroller.NewIrqTuningController(conf.AgentConfiguration, policyImplement, policyImplement.emitter, policyImplement.machineInfo)
		if err != nil {
			general.Errorf("failed to NewIrqTuningController, err %s", err)
			return false, agent.ComponentStub{}, err
		} else {
			policyImplement.irqTuner = irqTuner
		}
	}

	// register allocation behaviors for pods with different QoS level
	policyImplement.allocationHandlers = map[string]util.AllocationHandler{
		consts.PodAnnotationQoSLevelSharedCores:    policyImplement.sharedCoresAllocationHandler,
		consts.PodAnnotationQoSLevelDedicatedCores: policyImplement.dedicatedCoresAllocationHandler,
		consts.PodAnnotationQoSLevelReclaimedCores: policyImplement.reclaimedCoresAllocationHandler,
		consts.PodAnnotationQoSLevelSystemCores:    policyImplement.systemCoresAllocationHandler,
	}

	// register hint providers for pods with different QoS level
	policyImplement.hintHandlers = map[string]util.HintHandler{
		consts.PodAnnotationQoSLevelSharedCores:    policyImplement.sharedCoresHintHandler,
		consts.PodAnnotationQoSLevelDedicatedCores: policyImplement.dedicatedCoresHintHandler,
		consts.PodAnnotationQoSLevelReclaimedCores: policyImplement.reclaimedCoresHintHandler,
		consts.PodAnnotationQoSLevelSystemCores:    policyImplement.systemCoresHintHandler,
	}

	if err := policyImplement.cleanPools(); err != nil {
		return false, agent.ComponentStub{}, fmt.Errorf("cleanPools failed with error: %v", err)
	}

	if err := policyImplement.initReservePool(); err != nil {
		return false, agent.ComponentStub{}, fmt.Errorf("dynamic policy initReservePool failed with error: %v", err)
	}

	if err := policyImplement.initReclaimPool(); err != nil {
		return false, agent.ComponentStub{}, fmt.Errorf("dynamic policy initReclaimPool failed with error: %v", err)
	}

	if err := policyImplement.RegisterCPUSetAdjustmentHandler("bulkhead", policyImplement.bulkheadManager.RunCPUSetAdjustmentHandlers); err != nil {
		return false, agent.ComponentStub{}, fmt.Errorf("dynamic policy register bulkhead cpuset adjustment handler failed with error: %v", err)
	}

	if conf.EnableIRQTuner {
		if err := policyImplement.initInterruptPool(); err != nil {
			return false, agent.ComponentStub{}, fmt.Errorf("dynamic policy initInterruptPool failed with error: %v", err)
		}
	}

	err = agentCtx.MetaServer.ConfigurationManager.AddConfigWatcher(crd.AdminQoSConfigurationGVR)
	if err != nil {
		return false, nil, err
	}

	if err := agentCtx.MetaServer.ConfigurationManager.AddConfigWatcher(crd.StrategyGroupGVR); err != nil {
		return false, nil, err
	}

	err = agentCtx.ConfigurationManager.AddConfigWatcher(crd.IRQTuningConfigurationGVR)
	if err != nil {
		return false, nil, err
	}

	pluginWrapper, err := skeleton.NewRegistrationPluginWrapper(policyImplement, conf.QRMPluginSocketDirs, func(key string, value int64) {
		_ = wrappedEmitter.StoreInt64(key, value, metrics.MetricTypeNameRaw)
	})
	if err != nil {
		return false, agent.ComponentStub{}, fmt.Errorf("dynamic policy new plugin wrapper failed with error: %v", err)
	}

	return true, &agent.PluginWrapper{GenericPlugin: pluginWrapper}, nil
}

// topologyAllocationHook is an AllocationHook that intercepts allocation info changes and updates topology annotations.
// It generates and merges topology-aware CPU allocation annotations into the allocationInfo when there are physical
// CPU allocation changes, quantity changes, or NUMA-aware topology assignments changes.
func (p *DynamicPolicy) topologyAllocationHook(oldInfo, newInfo *state.AllocationInfo) error {
	if newInfo == nil || !newInfo.CheckMainContainer() || !newInfo.CheckNUMABinding() {
		return nil
	}

	if len(newInfo.TopologyAwareAssignments) == 0 {
		return nil
	}

	if !cpuutil.IsTopologyAllocationChanged(oldInfo, newInfo) {
		return nil
	}

	annotations, err := cpuutil.GetCPUTopologyAllocationsAnnotations(newInfo, p.topologyAllocationAnnotationKey)
	if err != nil {
		return err
	}

	newInfo.Annotations = general.MergeAnnotations(newInfo.Annotations, annotations)
	return nil
}

func (p *DynamicPolicy) Name() string {
	return p.name
}

func (p *DynamicPolicy) ResourceName() string {
	return string(v1.ResourceCPU)
}

func (p *DynamicPolicy) startKubeletPodCacheSyncDrivenCPUSetRetry() {
	if p.metaServer == nil || p.metaServer.MetaAgent == nil {
		return
	}
	registrar, ok := p.metaServer.PodFetcher.(podmeta.KubeletPodCacheSyncEventRegistrar)
	if !ok {
		return
	}
	events, unregister := registrar.RegisterKubeletPodCacheSyncListener("dynamic-policy-cpuset-adjustment")
	if events == nil {
		unregister()
		return
	}
	stopCh := p.stopCh
	go func(stop <-chan struct{}) {
		defer unregister()
		for {
			select {
			case event, ok := <-events:
				if !ok {
					return
				}
				if event.CgroupCreated {
					p.handleCgroupCreateEvent()
				}
			case <-stop:
				return
			}
		}
	}(stopCh)
}

func (p *DynamicPolicy) Start() (err error) {
	general.Infof("called")

	p.Lock()
	if p.started {
		general.Infof("is already started")
		p.Unlock()
		return nil
	}
	p.started = true
	p.stopCh = make(chan struct{})
	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryStopCh = p.stopCh
	p.cpuSetAdjustmentRetryStopping = false
	p.cpuSetAdjustmentRetryMu.Unlock()
	p.Unlock()

	defer func() {
		if err != nil {
			p.Lock()
			if p.started {
				p.started = false
				close(p.stopCh)
			}
			p.Unlock()
		}
	}()

	if err = p.prepareAdvisorPostCommitTargetOnStart(); err != nil {
		return fmt.Errorf("prepare pending advisor post-commit target: %w", err)
	}
	if p.hasAnyPendingAdvisorPostCommitTarget() {
		p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonApplyFailed)
	}

	if p.irqTuner != nil {
		go p.irqTuner.Run(p.stopCh)
	}
	p.startKubeletPodCacheSyncDrivenCPUSetRetry()

	go wait.Until(func() {
		_ = p.emitter.StoreInt64(util.MetricNameHeartBeat, 1, metrics.MetricTypeNameRaw)
	}, time.Second*30, p.stopCh)

	err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.ClearResidualState, general.HealthzCheckStateNotReady,
		qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.clearResidualState, stateCheckPeriod, healthCheckTolerationTimes)
	if err != nil {
		general.Errorf("start %v failed,err:%v", cpuconsts.ClearResidualState, err)
	}

	err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.CheckCPUSet, general.HealthzCheckStateNotReady,
		qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.checkCPUSet, cpusetCheckPeriod, healthCheckTolerationTimes)
	if err != nil {
		general.Errorf("start %v failed,err:%v", cpuconsts.CheckCPUSet, err)
	}

	err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.SyncSystemExclusivePool, general.HealthzCheckStateNotReady,
		qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.syncSystemExclusivePool, syncSystemExclusivePoolPeriod, healthCheckTolerationTimes)
	if err != nil {
		general.Errorf("start %v failed,err:%v", cpuconsts.SyncSystemExclusivePool, err)
	}

	err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.SyncBulkhead, general.HealthzCheckStateNotReady,
		qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.runBulkheadPeriodicalHandlers, syncBulkheadPeriod, healthCheckTolerationTimes)
	if err != nil {
		general.Errorf("start %v failed,err:%v", cpuconsts.SyncBulkhead, err)
	}

	// start cpu-idle syncing if needed
	if p.enableSyncingCPUIdle {
		general.Infof("syncCPUIdle enabled")

		if len(p.reclaimRelativeRootCgroupPaths) == 0 {
			return fmt.Errorf("enable syncing cpu idle but not set reclaiemd relative root cgroup path in configuration")
		}

		err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.SyncCPUIdle, general.HealthzCheckStateNotReady,
			qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.syncCPUIdle, syncCPUIdlePeriod, healthCheckTolerationTimes)
		if err != nil {
			general.Errorf("start %v failed,err:%v", cpuconsts.SyncCPUIdle, err)
		}
	}

	// start cpu burst sync if needed
	if p.enableCPUBurst {
		general.Infof("cpu burst is enabled")

		err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.SyncCPUBurst, general.HealthzCheckStateNotReady,
			qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.syncCPUBurst, syncCPUBurstPeriod, healthCheckTolerationTimes)
		if err != nil {
			general.Errorf("start %v failed,err:%v", cpuconsts.SyncCPUBurst, err)
		}
	}

	if p.conf.CPUQRMPluginConfig.EnableCPUWeight {
		general.Infof("cpu weight is enabled")

		err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.SyncCPUWeight, general.HealthzCheckStateNotReady,
			qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.syncCPUWeight, syncCPUWeightPeriod, healthCheckTolerationTimes)
		if err != nil {
			general.Errorf("start %v failed,err:%v", cpuconsts.SyncCPUWeight, err)
		}
	}

	// start cpu-pressure eviction plugin if needed
	if p.cpuPressureEviction != nil {
		p.Lock()
		if p.started {
			var ctx context.Context
			ctx, p.cpuPressureEvictionCancel = context.WithCancel(context.Background())
			go p.cpuPressureEviction.Run(ctx)
		}
		p.Unlock()
	}

	go wait.Until(func() {
		periodicalhandler.ReadyToStartHandlersByGroup(qrm.QRMCPUPluginPeriodicalHandlerGroupName)
	}, 5*time.Second, p.stopCh)

	// pre-check necessary dirs if sys-advisor is enabled
	if !p.enableCPUAdvisor {
		general.Infof("start dynamic policy cpu plugin without sys-advisor")
		return nil
	} else if p.cpuAdvisorSocketAbsPath == "" || p.cpuPluginSocketAbsPath == "" {
		return fmt.Errorf("invalid cpuAdvisorSocketAbsPath: %s or cpuPluginSocketAbsPath: %s",
			p.cpuAdvisorSocketAbsPath, p.cpuPluginSocketAbsPath)
	}

	general.Infof("start dynamic policy cpu plugin with sys-advisor")
	general.RegisterHeartbeatCheck(cpuconsts.CommunicateWithAdvisor, 2*time.Minute, general.HealthzCheckStateNotReady, 2*time.Minute)

	err = p.initAdvisorClientConn()
	if err != nil {
		general.Errorf("initAdvisorClientConn failed with error: %v", err)
		return
	}

	p.advisorMonitor, err = timemonitor.NewTimeMonitor(cpuAdvisorHealthMonitorName, cpuAdvisorHealthMonitorInterval,
		cpuAdvisorUnhealthyThreshold, cpuAdvisorHealthyThreshold,
		util.MetricNameAdvisorUnhealthy, p.emitter, cpuAdvisorHealthyCount, true)
	if err != nil {
		general.Errorf("initialize cpu advisor monitor failed with error: %v", err)
		return
	}
	go p.advisorMonitor.Run(p.stopCh)

	go wait.BackoffUntil(func() { p.serveForAdvisor(p.stopCh) }, wait.NewExponentialBackoffManager(
		800*time.Millisecond, 30*time.Second, 2*time.Minute, 2.0, 0, &clock.RealClock{}), true, p.stopCh)

	communicateWithCPUAdvisorServer := func() {
		general.Infof("waiting cpu plugin checkpoint server serving confirmation")
		if conn, err := process.Dial(p.cpuPluginSocketAbsPath, 5*time.Second); err != nil {
			general.Errorf("dial check at socket: %s failed with err: %v", p.cpuPluginSocketAbsPath, err)
			return
		} else {
			_ = conn.Close()
		}
		general.Infof("cpu plugin checkpoint server serving confirmed")

		p.getAdviceFromAdvisorLoop(p.stopCh)
		select {
		case <-p.stopCh:
			// stopCh closed, no need to fall back to ListAndWatch.
			return
		default:
		}

		general.Infof("advisor does not implement GetAdvice, fall back to ListAndWatch")

		if err := p.pushCPUAdvisor(); err != nil {
			general.Errorf("sync existing containers to cpu advisor failed with error: %v", err)
			return
		}
		general.Infof("sync existing containers to cpu advisor successfully")

		// call lw of CPUAdvisorServer and do allocation
		if err := p.lwCPUAdvisorServer(p.stopCh); err != nil {
			general.Errorf("lwCPUAdvisorServer failed with error: %v", err)
		} else {
			general.Infof("lwCPUAdvisorServer finished")
		}
	}

	go wait.BackoffUntil(communicateWithCPUAdvisorServer, wait.NewExponentialBackoffManager(800*time.Millisecond,
		30*time.Second, 2*time.Minute, 2.0, 0, &clock.RealClock{}), true, p.stopCh)

	err = p.resourcePackageManager.Run(p.stopCh)
	if err != nil {
		return fmt.Errorf("resourcePackageManager.Run failed with error: %v", err)
	}

	p.syncResourcePackagePinnedCPUSet()
	go wait.Until(p.syncResourcePackagePinnedCPUSet, 30*time.Second, p.stopCh)

	err = p.sharedCoresNUMABindingHintOptimizer.Run(p.stopCh)
	if err != nil {
		return fmt.Errorf("sharedCoresNUMABindingHintOptimizer.Run failed with error: %v", err)
	}

	err = p.dedicatedCoresNUMABindingHintOptimizer.Run(p.stopCh)
	if err != nil {
		return fmt.Errorf("dedicatedCoresNUMABindingHintOptimizer.Run failed with error: %v", err)
	}

	return nil
}

func (p *DynamicPolicy) Stop() error {
	p.Lock()
	if !p.started {
		general.Warningf("already stopped")
		p.Unlock()
		return nil
	}

	p.started = false
	stopCh := p.stopCh
	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryStopping = true
	p.cpuSetAdjustmentRetryMu.Unlock()
	close(stopCh)
	p.Unlock()
	p.cpuSetAdjustmentRetryWG.Wait()

	if p.cpuPressureEvictionCancel != nil {
		p.cpuPressureEvictionCancel()
	}

	periodicalhandler.StopHandlersByGroup(qrm.QRMCPUPluginPeriodicalHandlerGroupName)

	if p.advisorConn != nil {
		if err := p.advisorConn.Close(); err != nil {
			return err
		}
	}

	general.Infof("stopped")
	return nil
}

// GetResourcesAllocation returns allocation results of corresponding resources
func (p *DynamicPolicy) GetResourcesAllocation(_ context.Context,
	req *pluginapi.GetResourcesAllocationRequest,
) (*pluginapi.GetResourcesAllocationResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("GetResourcesAllocation got nil req")
	}

	general.Infof("called")
	p.Lock()
	defer p.Unlock()

	podEntries := p.state.GetPodEntries()
	machineState := p.state.GetMachineState()

	// rumpUpPooledCPUs is the total available cpu cores minus those that are reserved
	rumpUpPooledCPUs := machineState.GetFilteredAvailableCPUSet(p.reservedCPUs,
		func(ai *state.AllocationInfo) bool {
			return ai.CheckDedicated() || ai.CheckSharedNUMABinding()
		},
		state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckDedicatedNUMABinding))
	var allocationInfosJustFinishRampUp []*state.AllocationInfo
	needUpdateMachineState := false
	for podUID, containerEntries := range podEntries {
		// if it's a pool, not returning to QRM
		if containerEntries.IsPoolEntry() {
			continue
		}

		mainContainerAllocationInfo := podEntries[podUID].GetMainContainerEntry()
		for containerName, allocationInfo := range containerEntries {
			if allocationInfo == nil {
				continue
			}
			originAllocationInfo := allocationInfo
			allocationInfo = allocationInfo.Clone()

			// sync allocation info from main container to sidecar
			if allocationInfo.CheckSideCar() && mainContainerAllocationInfo != nil {
				if p.applySidecarAllocationInfoFromMainContainer(allocationInfo, mainContainerAllocationInfo) {
					general.Infof("pod: %s/%s, container: %s sync allocation info from main container",
						allocationInfo.PodNamespace, allocationInfo.PodName, containerName)
					if err := p.updateAllocationInfo(podUID, containerName, originAllocationInfo, allocationInfo, true); err != nil {
						general.Errorf("updateAllocationInfo failed for pod: %s/%s, container: %s: %v",
							allocationInfo.PodNamespace, allocationInfo.PodName, containerName, err)
						continue
					}
					needUpdateMachineState = true
				}
			}

			_, tsErr := time.Parse(util.QRMTimeFormat, allocationInfo.InitTimestamp)
			if tsErr != nil {
				if allocationInfo.CheckShared() && !allocationInfo.CheckNUMABinding() {
					general.Errorf("pod: %s/%s, container: %s init timestamp parsed failed with error: %v, re-ramp-up it",
						allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName, tsErr)

					rampUpReclaimFloor, err := p.deriveRampUpReclaimFloor(
						machineState, p.state.GetPodEntries(), true)
					if err != nil {
						return nil, fmt.Errorf("derive reclaim floor for legacy re-ramp-up failed: %w", err)
					}
					clonedPooledCPUs := rumpUpPooledCPUs.Difference(rampUpReclaimFloor)
					clonedPooledCPUsTopologyAwareAssignments, err := machine.GetNumaAwareAssignments(
						p.machineInfo.CPUTopology, clonedPooledCPUs)
					if err != nil {
						return nil, fmt.Errorf("get NUMA assignments for legacy re-ramp-up failed: %w", err)
					}

					allocationInfo.AllocationResult = clonedPooledCPUs
					allocationInfo.OriginalAllocationResult = clonedPooledCPUs
					allocationInfo.TopologyAwareAssignments = clonedPooledCPUsTopologyAwareAssignments
					allocationInfo.OriginalTopologyAwareAssignments = clonedPooledCPUsTopologyAwareAssignments
					// fill OwnerPoolName with empty string when ramping up
					allocationInfo.OwnerPoolName = commonstate.EmptyOwnerPoolName
					allocationInfo.RampUp = true
				}

				allocationInfo.InitTimestamp = time.Now().Format(util.QRMTimeFormat)
				if err := p.updateAllocationInfo(podUID, containerName, originAllocationInfo, allocationInfo, true); err != nil {
					general.Errorf("updateAllocationInfo failed for pod: %s/%s, container: %s: %v",
						allocationInfo.PodNamespace, allocationInfo.PodName, containerName, err)
				}
			} else if finishRampUp, _ := shouldAllocationFinishRampUp(allocationInfo, p.transitionPeriod, time.Now()); finishRampUp {
				general.Infof("pod: %s/%s, container: %s ramp up finished", allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName)
				allocationInfo.RampUp = false
				if allocationInfo.CheckShared() {
					p.state.SetAllocationInfo(podUID, containerName, allocationInfo, false)
					allocationInfosJustFinishRampUp = append(allocationInfosJustFinishRampUp, allocationInfo)
					continue
				}
				if err := p.updateAllocationInfo(podUID, containerName, originAllocationInfo, allocationInfo, true); err != nil {
					general.Errorf("updateAllocationInfo failed for pod: %s/%s, container: %s: %v",
						allocationInfo.PodNamespace, allocationInfo.PodName, containerName, err)
					continue
				}
			}

		}
	}

	if len(allocationInfosJustFinishRampUp) > 0 {
		if err := p.putAllocationsAndAdjustAllocationEntries(allocationInfosJustFinishRampUp, true, true); err != nil {
			for _, allocationInfo := range allocationInfosJustFinishRampUp {
				current := p.state.GetAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName)
				if current != nil && !current.RampUp &&
					current.OwnerPoolName != commonstate.EmptyOwnerPoolName {
					continue
				}
				allocationInfo = allocationInfo.Clone()
				allocationInfo.RampUp = true
				p.state.SetAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, allocationInfo, false)
			}
			// not influencing return response to kubelet when putAllocationsAndAdjustAllocationEntries failed
			general.Errorf("putAllocationsAndAdjustAllocationEntries failed with error: %v", err)
		}
	} else if needUpdateMachineState {
		// NOTE: we only need update machine state when putAllocationsAndAdjustAllocationEntries is skipped,
		// because putAllocationsAndAdjustAllocationEntries will update machine state.
		general.Infof("GetResourcesAllocation update machine state")
		podEntries = p.state.GetPodEntries()
		updatedMachineState, err := generateMachineStateFromPodEntries(p.machineInfo.CPUTopology, podEntries, machineState)
		if err != nil {
			general.Errorf("GetResourcesAllocation GenerateMachineStateFromPodEntries failed with error: %v", err)
			return nil, fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
		}
		p.state.SetMachineState(updatedMachineState, true)
	}

	podEntries = p.state.GetPodEntries()
	podResources := make(map[string]*pluginapi.ContainerResources)
	for podUID, containerEntries := range podEntries {
		if containerEntries.IsPoolEntry() {
			continue
		}

		if podResources[podUID] == nil {
			podResources[podUID] = &pluginapi.ContainerResources{}
		}

		for containerName, allocationInfo := range containerEntries {
			if allocationInfo == nil {
				general.Warningf("container %s allocation info is nil during GetResourcesAllocation, skip it", containerName)
				continue
			}
			if podResources[podUID].ContainerResources == nil {
				podResources[podUID].ContainerResources = make(map[string]*pluginapi.ResourceAllocation)
			}

			topologyAssignments := make(map[uint64]uint64)
			for numaID, cset := range allocationInfo.TopologyAwareAssignments {
				topologyAssignments[uint64(numaID)] = uint64(cset.Size())
			}

			podResources[podUID].ContainerResources[containerName] = &pluginapi.ResourceAllocation{
				ResourceAllocation: map[string]*pluginapi.ResourceAllocationInfo{
					string(v1.ResourceCPU): {
						OciPropertyName:     util.OCIPropertyNameCPUSetCPUs,
						IsNodeResource:      false,
						IsScalarResource:    true,
						AllocatedQuantity:   float64(allocationInfo.AllocationResult.Size()),
						AllocationResult:    allocationInfo.AllocationResult.String(),
						TopologyAssignments: topologyAssignments,
						Annotations:         general.DeepCopyMap(allocationInfo.Annotations),
					},
				},
			}
			if p.shouldBypassCPUSetAdjustmentForAllocation(allocationInfo) {
				clearCPUSetInAllocation(podResources[podUID].ContainerResources[containerName])
			}
		}
	}

	return &pluginapi.GetResourcesAllocationResponse{
		PodResources: podResources,
	}, nil
}

// GetTopologyAwareResources returns allocation results of corresponding resources as machineInfo aware format
func (p *DynamicPolicy) GetTopologyAwareResources(_ context.Context,
	req *pluginapi.GetTopologyAwareResourcesRequest,
) (*pluginapi.GetTopologyAwareResourcesResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("GetTopologyAwareResources got nil req")
	}

	general.Infof("called")
	p.RLock()
	defer p.RUnlock()

	allocationInfo := p.state.GetAllocationInfo(req.PodUid, req.ContainerName)
	if allocationInfo == nil {
		return nil, fmt.Errorf("pod: %s, container: %s is not show up in cpu plugin state", req.PodUid, req.ContainerName)
	}

	resp := &pluginapi.GetTopologyAwareResourcesResponse{
		PodUid:       allocationInfo.PodUid,
		PodName:      allocationInfo.PodName,
		PodNamespace: allocationInfo.PodNamespace,
		ContainerTopologyAwareResources: &pluginapi.ContainerTopologyAwareResources{
			ContainerName: allocationInfo.ContainerName,
		},
	}

	if allocationInfo.CheckSideCar() {
		resp.ContainerTopologyAwareResources.AllocatedResources = map[string]*pluginapi.TopologyAwareResource{
			string(v1.ResourceCPU): {
				IsNodeResource:                    false,
				IsScalarResource:                  true,
				AggregatedQuantity:                0,
				OriginalAggregatedQuantity:        0,
				TopologyAwareQuantityList:         nil,
				OriginalTopologyAwareQuantityList: nil,
			},
		}
	} else {
		resp.ContainerTopologyAwareResources.AllocatedResources = map[string]*pluginapi.TopologyAwareResource{
			string(v1.ResourceCPU): {
				IsNodeResource:                    false,
				IsScalarResource:                  true,
				AggregatedQuantity:                float64(allocationInfo.AllocationResult.Size()),
				OriginalAggregatedQuantity:        float64(allocationInfo.OriginalAllocationResult.Size()),
				TopologyAwareQuantityList:         util.GetTopologyAwareQuantityFromAssignments(allocationInfo.TopologyAwareAssignments),
				OriginalTopologyAwareQuantityList: util.GetTopologyAwareQuantityFromAssignments(allocationInfo.OriginalTopologyAwareAssignments),
			},
		}
	}

	return resp, nil
}

// GetTopologyAwareAllocatableResources returns corresponding allocatable resources as machineInfo aware format
func (p *DynamicPolicy) GetTopologyAwareAllocatableResources(_ context.Context,
	_ *pluginapi.GetTopologyAwareAllocatableResourcesRequest,
) (*pluginapi.GetTopologyAwareAllocatableResourcesResponse, error) {
	general.Infof("is called")

	numaNodes := p.machineInfo.CPUDetails.NUMANodes().ToSliceInt()
	topologyAwareAllocatableQuantityList := make([]*pluginapi.TopologyAwareQuantity, 0, len(numaNodes))
	topologyAwareCapacityQuantityList := make([]*pluginapi.TopologyAwareQuantity, 0, len(numaNodes))

	for _, numaNode := range numaNodes {
		numaNodeCPUs := p.machineInfo.CPUDetails.CPUsInNUMANodes(numaNode).Clone()
		topologyAwareAllocatableQuantityList = append(topologyAwareAllocatableQuantityList, &pluginapi.TopologyAwareQuantity{
			ResourceValue: float64(numaNodeCPUs.Difference(p.reservedCPUs).Size()),
			Node:          uint64(numaNode),
		})
		topologyAwareCapacityQuantityList = append(topologyAwareCapacityQuantityList, &pluginapi.TopologyAwareQuantity{
			ResourceValue: float64(numaNodeCPUs.Size()),
			Node:          uint64(numaNode),
		})
	}

	allocatableResources := map[string]*pluginapi.AllocatableTopologyAwareResource{
		string(v1.ResourceCPU): {
			IsNodeResource:                       false,
			IsScalarResource:                     true,
			AggregatedAllocatableQuantity:        float64(p.machineInfo.NumCPUs - p.reservedCPUs.Size()),
			TopologyAwareAllocatableQuantityList: topologyAwareAllocatableQuantityList,
			AggregatedCapacityQuantity:           float64(p.machineInfo.NumCPUs),
			TopologyAwareCapacityQuantityList:    topologyAwareCapacityQuantityList,
		},
	}

	p.addReclaimedCPUAllocatable(allocatableResources, numaNodes, p.reclaimConsumersForKCNR)

	return &pluginapi.GetTopologyAwareAllocatableResourcesResponse{
		AllocatableResources: allocatableResources,
	}, nil
}

// addReclaimedCPUAllocatable inserts a ReclaimedResourceMilliCPU entry
// into allocatableResources when ReclaimConsumersForKCNR is configured.
// The reported value is (headroom_cores * 1000 * summed_percentage / 100),
// converting cores to millicpu; summed_percentage is the sum of
// GetReclaimedPercentage across every configured consumer name, capped at
// 100 (unknown names contribute 0 and are logged by GetReclaimedPercentage).
func (p *DynamicPolicy) addReclaimedCPUAllocatable(
	allocatableResources map[string]*pluginapi.AllocatableTopologyAwareResource,
	numaNodes []int,
	consumerNames []string,
) {
	if !p.conf.EnableReclaimedResourceAllocatableReporting {
		return
	}

	if len(consumerNames) == 0 {
		return
	}

	numaHeadroom := p.state.GetNUMAHeadroom()
	reclaimedNUMAHeadroom := reclaim.GetReclaimedNUMAHeadroom(
		numaHeadroom, p.dynamicConfig.GetDynamicConfiguration(), consumerNames...)

	topologyAwareList := make([]*pluginapi.TopologyAwareQuantity, 0, len(numaNodes))
	for _, numaNode := range numaNodes {
		topologyAwareList = append(topologyAwareList, &pluginapi.TopologyAwareQuantity{
			ResourceValue: reclaimedNUMAHeadroom[numaNode] * 1000,
			Node:          uint64(numaNode),
		})
	}

	var totalReclaimedHeadroom float64
	for _, v := range reclaimedNUMAHeadroom {
		totalReclaimedHeadroom += v
	}
	aggregated := totalReclaimedHeadroom * 1000

	allocatableResources[string(consts.ReclaimedResourceMilliCPU)] = &pluginapi.AllocatableTopologyAwareResource{
		IsNodeResource:                       false,
		IsScalarResource:                     true,
		AggregatedAllocatableQuantity:        aggregated,
		TopologyAwareAllocatableQuantityList: topologyAwareList,
		AggregatedCapacityQuantity:           aggregated,
		TopologyAwareCapacityQuantityList:    topologyAwareList,
	}
}

// GetTopologyHints returns hints of corresponding resources
func (p *DynamicPolicy) GetTopologyHints(ctx context.Context,
	req *pluginapi.ResourceRequest,
) (resp *pluginapi.ResourceHintsResponse, err error) {
	if req == nil {
		return nil, fmt.Errorf("GetTopologyHints got nil req")
	}

	// identify if the pod is a debug pod,
	// if so, apply specific strategy to it.
	// since GetKatalystQoSLevelFromResourceReq function will filter annotations,
	// we should do it before GetKatalystQoSLevelFromResourceReq.
	isDebugPod := util.IsDebugPod(req.Annotations, p.podDebugAnnoKeys)

	qosLevel, err := util.GetKatalystQoSLevelFromResourceReq(p.qosConfig, req, p.podAnnotationKeptKeys, p.podLabelKeptKeys)
	if err != nil {
		err = fmt.Errorf("GetKatalystQoSLevelFromResourceReq for pod: %s/%s, container: %s failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		general.Errorf("%s", err.Error())
		return nil, err
	}

	reqInt, reqFloat64, err := util.GetQuantityFromResourceReq(req)
	if err != nil {
		return nil, fmt.Errorf("getReqQuantityFromResourceReq failed with error: %v", err)
	}

	general.InfoS("called",
		"podNamespace", req.PodNamespace,
		"podName", req.PodName,
		"containerName", req.ContainerName,
		"podType", req.PodType,
		"podRole", req.PodRole,
		"containerType", req.ContainerType,
		"qosLevel", qosLevel,
		"numCPUsInt", reqInt,
		"numCPUsFloat64", reqFloat64,
		"isDebugPod", isDebugPod,
		"annotation", req.Annotations)

	if req.ContainerType == pluginapi.ContainerType_INIT || isDebugPod {
		general.Infof("there is no NUMA preference, return nil hint")
		return util.PackResourceHintsResponse(req, string(v1.ResourceCPU),
			map[string]*pluginapi.ListOfTopologyHints{
				string(v1.ResourceCPU): nil, // indicates that there is no numa preference
			})
	}

	startTime := time.Now()
	p.RLock()
	defer func() {
		p.RUnlock()
		if err != nil {
			inplaceUpdateResizing := util.PodInplaceUpdateResizing(req)
			_ = p.emitter.StoreInt64(util.MetricNameGetTopologyHintsFailed, 1, metrics.MetricTypeNameRaw,
				metrics.MetricTag{Key: "error_message", Val: metric.MetricTagValueFormat(err)},
				metrics.MetricTag{Key: util.MetricTagNameInplaceUpdateResizing, Val: strconv.FormatBool(inplaceUpdateResizing)})

			general.ErrorS(err, "GetTopologyHints failed",
				"podNamespace", req.PodNamespace,
				"podName", req.PodName,
				"containerName", req.ContainerName,
				"inplaceUpdateResizing", inplaceUpdateResizing,
			)
		}
		general.InfoS("finished",
			"duration", time.Since(startTime).String(),
			"podNamespace", req.PodNamespace,
			"podName", req.PodName,
			"containerName", req.ContainerName,
		)
	}()

	if p.hintHandlers[qosLevel] == nil {
		return nil, fmt.Errorf("katalyst QoS level: %s is not supported yet", qosLevel)
	}
	return p.hintHandlers[qosLevel](ctx, req)
}

// GetPodTopologyHints returns hints of corresponding resources for pod
func (p *DynamicPolicy) GetPodTopologyHints(ctx context.Context,
	req *pluginapi.PodResourceRequest,
) (resp *pluginapi.PodResourceHintsResponse, err error) {
	return nil, util.ErrNotImplemented
}

// GetResourcePluginOptions returns options to be communicated with Resource Manager
func (p *DynamicPolicy) GetResourcePluginOptions(context.Context,
	*pluginapi.Empty,
) (*pluginapi.ResourcePluginOptions, error) {
	general.Infof("called")
	return &pluginapi.ResourcePluginOptions{
		PreStartRequired:      false,
		WithTopologyAlignment: true,
		NeedReconcile:         true,
	}, nil
}

// Allocate is called during pod admit so that the resource
// plugin can allocate corresponding resource for the container
// according to resource request
func (p *DynamicPolicy) Allocate(ctx context.Context,
	req *pluginapi.ResourceRequest,
) (resp *pluginapi.ResourceAllocationResponse, respErr error) {
	if req == nil {
		return nil, fmt.Errorf("allocate got nil req")
	}
	ctx = withAllocationPodMeta(ctx, req)

	// identify if the pod is a debug pod,
	// if so, apply specific strategy to it.
	// since GetKatalystQoSLevelFromResourceReq function will filter annotations,
	// we should do it before GetKatalystQoSLevelFromResourceReq.
	isDebugPod := util.IsDebugPod(req.Annotations, p.podDebugAnnoKeys)

	existReallocAnno, isReallocation := util.IsReallocation(req.Annotations)

	qosLevel, err := util.GetKatalystQoSLevelFromResourceReq(p.qosConfig, req, p.podAnnotationKeptKeys, p.podLabelKeptKeys)
	if err != nil {
		err = fmt.Errorf("GetKatalystQoSLevelFromResourceReq for pod: %s/%s, container: %s failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		general.Errorf("%s", err.Error())
		return nil, err
	}

	reqInt, reqFloat64, err := util.GetQuantityFromResourceReq(req)
	if err != nil {
		return nil, fmt.Errorf("getReqQuantityFromResourceReq failed with error: %v", err)
	}

	general.InfoS("called",
		"podNamespace", req.PodNamespace,
		"podName", req.PodName,
		"containerName", req.ContainerName,
		"podType", req.PodType,
		"podRole", req.PodRole,
		"containerType", req.ContainerType,
		"qosLevel", qosLevel,
		"numCPUsInt", reqInt,
		"numCPUsFloat64", reqFloat64,
		"isDebugPod", isDebugPod,
		"annotations", req.Annotations)

	if req.ContainerType == pluginapi.ContainerType_INIT {
		return &pluginapi.ResourceAllocationResponse{
			PodUid:         req.PodUid,
			PodNamespace:   req.PodNamespace,
			PodName:        req.PodName,
			ContainerName:  req.ContainerName,
			ContainerType:  req.ContainerType,
			ContainerIndex: req.ContainerIndex,
			PodRole:        req.PodRole,
			PodType:        req.PodType,
			ResourceName:   string(v1.ResourceCPU),
			Labels:         general.DeepCopyMap(req.Labels),
			Annotations:    general.DeepCopyMap(req.Annotations),
		}, nil
	} else if isDebugPod {
		return &pluginapi.ResourceAllocationResponse{
			PodUid:         req.PodUid,
			PodNamespace:   req.PodNamespace,
			PodName:        req.PodName,
			ContainerName:  req.ContainerName,
			ContainerType:  req.ContainerType,
			ContainerIndex: req.ContainerIndex,
			PodRole:        req.PodRole,
			PodType:        req.PodType,
			ResourceName:   string(v1.ResourceCPU),
			AllocationResult: &pluginapi.ResourceAllocation{
				ResourceAllocation: map[string]*pluginapi.ResourceAllocationInfo{
					string(v1.ResourceCPU): {
						// return ResourceAllocation with empty OciPropertyName, AllocatedQuantity, AllocationResult for containers in debug pod,
						// it won't influence oci spec properties of the container
						IsNodeResource:   false,
						IsScalarResource: true,
					},
				},
			},
			Labels:      general.DeepCopyMap(req.Labels),
			Annotations: general.DeepCopyMap(req.Annotations),
		}, nil
	}

	startTime := time.Now()
	unlockAllocationRequest := p.lockAllocationRequest(req.PodUid, req.ContainerName)
	defer unlockAllocationRequest()
	p.Lock()
	rollbackSnapshot := allocationRollbackSnapshot{
		revision:                p.state.GetRevision(),
		podEntries:              p.state.GetPodEntries(),
		machineState:            p.state.GetMachineState(),
		allowOverlap:            p.state.GetAllowSharedCoresOverlapReclaimedCores(),
		disableDedicatedOverlap: p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
	}
	defer func() {
		// calls sys-advisor to inform the latest container
		if p.enableCPUAdvisor && respErr == nil && req.ContainerType != pluginapi.ContainerType_INIT {
			_, err := p.advisorClient.AddContainer(ctx, &advisorsvc.ContainerMetadata{
				PodUid:               req.PodUid,
				PodNamespace:         req.PodNamespace,
				PodName:              req.PodName,
				ContainerName:        req.ContainerName,
				ContainerType:        req.ContainerType,
				ContainerIndex:       req.ContainerIndex,
				Labels:               maputil.CopySS(req.Labels),
				Annotations:          maputil.CopySS(req.Annotations),
				QosLevel:             qosLevel,
				RequestQuantity:      uint64(reqInt),
				RequestMilliQuantity: uint64(reqFloat64 * 1000),
				UseMilliQuantity:     true,
			})
			if err != nil {
				resp = nil
				respErr = fmt.Errorf("add container to qos aware server failed with error: %v", err)
			}
		}
		if respErr != nil {
			inplaceUpdateResizing := util.PodInplaceUpdateResizing(req)
			var compensated *requestStateCompensatedError
			if !errors.As(respErr, &compensated) {
				if err := p.rollbackAllocationState(req, rollbackSnapshot); err != nil {
					respErr = fmt.Errorf("%w; allocation rollback failed: %v", respErr, err)
				}
			}

			metricTags := []metrics.MetricTag{
				{Key: "error_message", Val: metric.MetricTagValueFormat(respErr)},
				{Key: util.MetricTagNameInplaceUpdateResizing, Val: strconv.FormatBool(inplaceUpdateResizing)},
			}
			if existReallocAnno {
				metricTags = append(metricTags, metrics.MetricTag{Key: "reallocation", Val: isReallocation})
			}
			_ = p.emitter.StoreInt64(util.MetricNameAllocateFailed, 1, metrics.MetricTypeNameRaw, metricTags...)
		}
		if err := p.state.StoreState(); err != nil {
			general.ErrorS(err, "store state failed", "podName", req.PodName, "containerName", req.ContainerName)
			resp = nil
			if respErr == nil {
				respErr = fmt.Errorf("store allocation state failed: %w", err)
				if rollbackErr := p.rollbackAllocationState(req, rollbackSnapshot); rollbackErr != nil {
					respErr = fmt.Errorf("%w; allocation rollback failed: %v", respErr, rollbackErr)
				}
			} else {
				respErr = fmt.Errorf("%w; store allocation state failed: %v", respErr, err)
			}
		}

		p.Unlock()
		if respErr != nil {
			general.ErrorS(respErr, "Allocate failed",
				"podNamespace", req.PodNamespace,
				"podName", req.PodName,
				"containerName", req.ContainerName,
			)
		}
		general.InfoS("finished",
			"duration", time.Since(startTime).String(),
			"podNamespace", req.PodNamespace,
			"podName", req.PodName,
			"containerName", req.ContainerName,
		)
		return
	}()

	allocationInfo := p.state.GetAllocationInfo(req.PodUid, req.ContainerName)
	if allocationInfo != nil && allocationInfo.OriginalAllocationResult.Size() >= reqInt && !util.PodInplaceUpdateResizing(req) {
		general.InfoS("already allocated and meet requirement",
			"podNamespace", req.PodNamespace,
			"podName", req.PodName,
			"containerName", req.ContainerName,
			"numCPUs", reqInt,
			"originalAllocationResult", allocationInfo.OriginalAllocationResult.String(),
			"currentResult", allocationInfo.AllocationResult.String())

		resp, err = cpuutil.PackAllocationResponse(allocationInfo, string(v1.ResourceCPU), util.OCIPropertyNameCPUSetCPUs,
			false, true, req, allocationInfo.Annotations)
		if err != nil {
			general.Errorf("pod: %s/%s, container: %s PackResourceAllocationResponseByAllocationInfo failed with error: %v",
				req.PodNamespace, req.PodName, req.ContainerName, err)
			return nil, fmt.Errorf("PackResourceAllocationResponseByAllocationInfo failed with error: %v", err)
		}
		p.clearCPUSetInAllocationResponseIfNeeded(resp, allocationInfo)

		return resp, nil
	}

	if p.allocationHandlers[qosLevel] == nil {
		return nil, fmt.Errorf("katalyst QoS level: %s is not supported yet", qosLevel)
	}
	return p.allocationHandlers[qosLevel](ctx, req, false)
}

// AllocateForPod is called during pod admit so that the resource
// plugin can allocate corresponding resource for the pod
// according to resource request
func (p *DynamicPolicy) AllocateForPod(ctx context.Context,
	req *pluginapi.PodResourceRequest,
) (resp *pluginapi.PodResourceAllocationResponse, respErr error) {
	return nil, util.ErrNotImplemented
}

// PreStartContainer is called, if indicated by resource plugin during registration phase,
// before each container start. Resource plugin can run resource specific operations
// such as resetting the resource before making resources available to the container
func (p *DynamicPolicy) PreStartContainer(context.Context,
	*pluginapi.PreStartContainerRequest,
) (*pluginapi.PreStartContainerResponse, error) {
	return nil, nil
}

func (p *DynamicPolicy) RemovePod(ctx context.Context,
	req *pluginapi.RemovePodRequest,
) (resp *pluginapi.RemovePodResponse, err error) {
	if req == nil {
		return nil, fmt.Errorf("RemovePod got nil req")
	}
	general.InfoS("called", "podUID", req.PodUid)

	startTime := time.Now()
	p.Lock()
	defer func() {
		p.Unlock()
		if err != nil {
			_ = p.emitter.StoreInt64(util.MetricNameRemovePodFailed, 1, metrics.MetricTypeNameRaw,
				metrics.MetricTag{Key: "error_message", Val: metric.MetricTagValueFormat(err)})
			general.ErrorS(err, "RemovePod failed", "podUID", req.PodUid)
		}
		general.InfoS("finished", "duration", time.Since(startTime).String(), "podUID", req.PodUid)
	}()

	currentPodEntries := p.state.GetPodEntries()
	if len(currentPodEntries[req.PodUid]) == 0 {
		if err := AccompanyResourceRegistry.ReleaseAccompanyResource(req); err != nil {
			general.ErrorS(err, "failed to release accompany resource", "podUID", req.PodUid)
			return nil, fmt.Errorf("failed to release accompany resource: %w", err)
		}
		return &pluginapi.RemovePodResponse{}, nil
	}

	if p.enableCPUAdvisor {
		if p.advisorClient == nil {
			return nil, fmt.Errorf("cpu advisor client is nil")
		}
		_, err = p.advisorClient.RemovePod(ctx, &advisorsvc.RemovePodRequest{PodUid: req.PodUid})
		if err != nil {
			return nil, fmt.Errorf("remove pod in QoS aware server failed with error: %v", err)
		}
	}

	expectedRevision := p.state.GetRevision()
	podEntries := currentPodEntries.Clone()
	delete(podEntries, req.PodUid)
	p.cleanPoolsFromPodEntries(podEntries)
	machineState, err := generateMachineStateFromPodEntries(
		p.machineInfo.CPUTopology, podEntries, p.state.GetMachineState())
	if err != nil {
		return nil, fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
	}

	err = p.adjustAllocationEntriesAtRevision(
		podEntries, machineState, true, expectedRevision)
	if err != nil {
		general.ErrorS(err, "adjustAllocationEntries failed", "podUID", req.PodUid)
		err = p.persistPodDeletionAfterAdjustFailure(
			err, podEntries, machineState, expectedRevision)
		if err != nil {
			return nil, fmt.Errorf("commit pod removal failed: %w", err)
		}
	}

	if err := AccompanyResourceRegistry.ReleaseAccompanyResource(req); err != nil {
		general.ErrorS(err, "failed to release accompany resource", "podUID", req.PodUid)
		return nil, fmt.Errorf("failed to release accompany resource: %w", err)
	}

	return &pluginapi.RemovePodResponse{}, nil
}

func (p *DynamicPolicy) removePod(podUID string, podEntries state.PodEntries, persistCheckpoint bool) error {
	delete(podEntries, podUID)

	updatedMachineState, err := generateMachineStateFromPodEntries(p.machineInfo.CPUTopology, podEntries, p.state.GetMachineState())
	if err != nil {
		return fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
	}

	p.state.SetPodEntries(podEntries, false)
	p.state.SetMachineState(updatedMachineState, false)
	if persistCheckpoint {
		return p.state.StoreState()
	}
	return nil
}

func (p *DynamicPolicy) removeContainer(podUID, containerName string, persistCheckpoint bool) error {
	podEntries := p.state.GetPodEntries()

	found := false
	if podEntries[podUID][containerName] != nil {
		found = true
	}

	delete(podEntries[podUID], containerName)

	if !found {
		return nil
	}

	updatedMachineState, err := generateMachineStateFromPodEntries(p.machineInfo.CPUTopology, podEntries, p.state.GetMachineState())
	if err != nil {
		return fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
	}

	p.state.SetPodEntries(podEntries, false)
	p.state.SetMachineState(updatedMachineState, false)
	if persistCheckpoint {
		return p.state.StoreState()
	}
	return nil
}

// initAdvisorClientConn initializes cpu-advisor related connections
func (p *DynamicPolicy) initAdvisorClientConn() (err error) {
	cpuAdvisorConn, err := process.Dial(
		p.cpuAdvisorSocketAbsPath,
		5*time.Second,
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			// add metadata to outgoing context to indicate that qrm supports GetAdvice.
			// advisor that also supports GetAdvice will ignore such AddContainer/RemovePod requests.
			ctx = metadata.AppendToOutgoingContext(ctx, util.AdvisorRPCMetadataKeySupportsGetAdvice, util.AdvisorRPCMetadataValueSupportsGetAdvice)
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
	)
	if err != nil {
		err = fmt.Errorf("get cpu advisor connection with socket: %s failed with error: %v", p.cpuAdvisorSocketAbsPath, err)
		return
	}

	p.advisorClient = advisorapi.NewCPUAdvisorClient(cpuAdvisorConn)
	p.advisorConn = cpuAdvisorConn
	return nil
}

func (p *DynamicPolicy) initHintOptimizers() error {
	var err error
	hintOptimizerFactoryOptions := p.generateHintOptimizerFactoryOptions()

	p.sharedCoresNUMABindingHintOptimizer, err = registry.SharedCoresHintOptimizerRegistry.HintOptimizerWithFilters(
		p.conf.SharedCoresHintOptimizerPolicies,
		p.conf.SharedCoresHintFilterPolicies,
		hintOptimizerFactoryOptions,
	)
	if err != nil {
		return fmt.Errorf("SharedCoresHintOptimizerRegistry.HintOptimizerWithFilters failed with error: %v", err)
	}

	p.dedicatedCoresNUMABindingHintOptimizer, err = registry.DedicatedCoresHintOptimizerRegistry.HintOptimizer(p.conf.DedicatedCoresHintOptimizerPolicies,
		p.generateHintOptimizerFactoryOptions())
	if err != nil {
		return fmt.Errorf("DedicatedCoresHintOptimizerRegistry.HintOptimizer failed with error: %v", err)
	}

	return nil
}

func (p *DynamicPolicy) generateHintOptimizerFactoryOptions() policy.HintOptimizerFactoryOptions {
	return policy.HintOptimizerFactoryOptions{
		Conf:                   p.conf,
		Emitter:                p.emitter,
		MetaServer:             p.metaServer,
		ResourcePackageManager: p.resourcePackageManager,
		State:                  p.state,
		ReservedCPUs:           p.reservedCPUs,
	}
}

// cleanPools is used to clean pools-related data in local state
func (p *DynamicPolicy) cleanPools() error {
	podEntries := p.state.GetPodEntries()
	poolsToDelete := p.cleanPoolsFromPodEntries(podEntries)
	if poolsToDelete.Len() == 0 {
		general.Infof("there is no pool to delete")
		return nil
	}

	general.Infof("pools to delete: %v", poolsToDelete.UnsortedList())
	machineState, err := generateMachineStateFromPodEntries(p.machineInfo.CPUTopology, podEntries, p.state.GetMachineState())
	if err != nil {
		return fmt.Errorf("calculate machineState by podEntries failed with error: %v", err)
	}

	p.state.SetPodEntries(podEntries, false)
	p.state.SetMachineState(machineState, false)
	if err := p.state.StoreState(); err != nil {
		general.ErrorS(err, "store state failed")
	}
	return nil
}

func (p *DynamicPolicy) cleanPoolsFromPodEntries(podEntries state.PodEntries) sets.String {
	remainPools := make(map[string]bool)

	// walk through pod entries to put them into specified pool maps
	for podUID, entries := range podEntries {
		if entries.IsPoolEntry() {
			continue
		}

		for containerName, allocationInfo := range entries {
			if allocationInfo == nil {
				general.Warningf("pod %s container %s allocation info is nil during cleanPools, skip it", podUID, containerName)
				continue
			}
			ownerPool := allocationInfo.GetOwnerPoolName()
			if ownerPool != commonstate.EmptyOwnerPoolName {
				remainPools[ownerPool] = true
			}
		}
	}

	// when default share residual backfill is enabled, the share pool is
	// synthesized without any owning container, so it must be retained here.
	keepSyntheticDefaultShare := p.dynamicConfig.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs

	// if pool exists in entries, but has no corresponding container, we need to delete it
	poolsToDelete := sets.NewString()
	for poolName, entries := range podEntries {
		if entries.IsPoolEntry() {
			// system pool is managed separately, should skip it
			if commonstate.IsSystemPool(poolName) {
				continue
			}
			// when default share residual backfill is enabled, the share pool is
			// synthesized without any owning container, so retain it here instead of
			// unconditionally adding share to state.ResidentPools (which would change
			// the legacy behavior when the gate is disabled).
			if keepSyntheticDefaultShare && poolName == commonstate.PoolNameShare {
				continue
			}
			if !remainPools[poolName] && !state.ResidentPools.Has(poolName) {
				poolsToDelete.Insert(poolName)
			}
		}
	}

	for _, poolName := range poolsToDelete.UnsortedList() {
		delete(podEntries, poolName)
	}
	return poolsToDelete
}

// initReservePool initializes reserve pool for system cores workload
func (p *DynamicPolicy) initReservePool() error {
	reserveAllocationInfo := p.state.GetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName)
	if reserveAllocationInfo != nil && !reserveAllocationInfo.AllocationResult.IsEmpty() {
		general.Infof("pool: %s allocation result transform from %s to %s",
			commonstate.PoolNameReserve, reserveAllocationInfo.AllocationResult.String(), p.reservedCPUs)
	}

	general.Infof("initReservePool %s: %s", commonstate.PoolNameReserve, p.reservedCPUs)
	topologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, p.reservedCPUs)
	if err != nil {
		return fmt.Errorf("unable to calculate topologyAwareAssignments for pool: %s, result cpuset: %s, error: %v",
			commonstate.PoolNameReserve, p.reservedCPUs.String(), err)
	}

	curReserveAllocationInfo := &state.AllocationInfo{
		AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReserve),
		AllocationResult:                 p.reservedCPUs.Clone(),
		OriginalAllocationResult:         p.reservedCPUs.Clone(),
		TopologyAwareAssignments:         topologyAwareAssignments,
		OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(topologyAwareAssignments),
	}
	p.state.SetAllocationInfo(commonstate.PoolNameReserve, commonstate.FakedContainerName, curReserveAllocationInfo, true)

	return nil
}

// initReclaimPool initializes pools for reclaimed-cores.
// if this info already exists in state-file, just use it, otherwise calculate right away
func (p *DynamicPolicy) initReclaimPool() error {
	// for reclaimed pool, we must make them exist when the node isn't in hybrid mode even if cause overlap
	allAvailableCPUs := p.machineInfo.CPUDetails.CPUs().Difference(p.reservedCPUs)
	defaultReservedReclaimedCPUSet, _, tErr := calculator.TakeHTByNUMABalance(p.machineInfo, allAvailableCPUs, p.reservedReclaimedCPUsSize)
	if tErr != nil {
		return fmt.Errorf("fallback TakeHTByNUMABalance faild in generatePoolsAndIsolation for defaultReservedReclaimedCPUSet with error: %v", tErr)
	}
	p.reservedReclaimedCPUSet = defaultReservedReclaimedCPUSet.Clone()

	defaultReservedTopologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, defaultReservedReclaimedCPUSet)
	if err != nil {
		return fmt.Errorf("unable to calculate defaultReservedTopologyAwareAssignments for pool: %s, "+
			"result cpuset: %s, error: %v", commonstate.PoolNameReclaim, defaultReservedReclaimedCPUSet.String(), err)
	}
	p.reservedReclaimedTopologyAwareAssignments = machine.DeepcopyCPUAssignment(defaultReservedTopologyAwareAssignments)

	reclaimedAllocationInfo := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	if reclaimedAllocationInfo == nil {
		podEntries := p.state.GetPodEntries()
		noneResidentCPUs := podEntries.GetFilteredPoolsCPUSet(state.ResidentPools)

		machineState := p.state.GetMachineState()
		availableCPUs := machineState.GetFilteredAvailableCPUSet(p.reservedCPUs,
			func(ai *state.AllocationInfo) bool {
				return ai.CheckDedicated() || ai.CheckSharedNUMABinding()
			},
			state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckDedicatedNUMABinding)).Difference(noneResidentCPUs)

		var initReclaimedCPUSetSize int
		if availableCPUs.Size() >= p.reservedReclaimedCPUsSize {
			initReclaimedCPUSetSize = p.reservedReclaimedCPUsSize
		} else {
			initReclaimedCPUSetSize = availableCPUs.Size()
		}

		reclaimedCPUSet, _, err := calculator.TakeHTByNUMABalance(p.machineInfo, availableCPUs, initReclaimedCPUSetSize)
		if err != nil {
			return fmt.Errorf("takeByNUMABalance faild in initReclaimPool for %s and %s with error: %v",
				commonstate.PoolNameShare, commonstate.PoolNameReclaim, err)
		}

		// for residual pools, we must make them exist even if cause overlap
		// todo: noneResidentCPUs is the same as reservedCPUs, why should we do this?
		if reclaimedCPUSet.IsEmpty() {
			reclaimedCPUSet = p.reservedReclaimedCPUSet.Clone()
		}

		general.Infof("initReclaimPool %s: %s", commonstate.PoolNameReclaim, reclaimedCPUSet.String())
		topologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, reclaimedCPUSet)
		if err != nil {
			return fmt.Errorf("unable to calculate topologyAwareAssignments for pool: %s, "+
				"result cpuset: %s, error: %v", commonstate.PoolNameReclaim, reclaimedCPUSet.String(), err)
		}

		curPoolAllocationInfo := &state.AllocationInfo{
			AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
			AllocationResult:                 reclaimedCPUSet.Clone(),
			OriginalAllocationResult:         reclaimedCPUSet.Clone(),
			TopologyAwareAssignments:         topologyAwareAssignments,
			OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(topologyAwareAssignments),
		}
		p.state.SetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName, curPoolAllocationInfo, true)
	} else {
		general.Infof("exist initial %s: %s", commonstate.PoolNameReclaim, reclaimedAllocationInfo.AllocationResult.String())
	}

	return nil
}

func (p *DynamicPolicy) initInterruptPool() error {
	interruptAllocationInfo := p.state.GetAllocationInfo(commonstate.PoolNameInterrupt, commonstate.FakedContainerName)
	if interruptAllocationInfo == nil {
		allocationInfo := &state.AllocationInfo{
			AllocationMeta: commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameInterrupt),
		}
		p.state.SetAllocationInfo(commonstate.PoolNameInterrupt, commonstate.FakedContainerName, allocationInfo, true)
	} else {
		general.Infof("exist initial %s: %s", commonstate.PoolNameInterrupt, interruptAllocationInfo.AllocationResult.String())
	}

	return nil
}

// getContainerRequestedCores parses and returns request cores for the given container
func (p *DynamicPolicy) getContainerRequestedCores(allocationInfo *state.AllocationInfo) float64 {
	return cpuutil.GetContainerRequestedCores(p.metaServer, allocationInfo)
}

func (p *DynamicPolicy) checkNonBindingShareCoresCpuResource(req *pluginapi.ResourceRequest) (bool, error) {
	_, reqFloat64, err := util.GetPodAggregatedRequestResource(req)
	if err != nil {
		return false, fmt.Errorf("GetQuantityFromResourceReq failed with error: %v", err)
	}

	shareCoresAllocatedInt := state.GetNonBindingSharedRequestedQuantityFromPodEntries(p.state.GetPodEntries(), map[string]float64{req.PodUid: reqFloat64}, p.getContainerRequestedCores)

	machineState := p.state.GetMachineState()
	pooledCPUs := machineState.GetFilteredAvailableCPUSet(p.reservedCPUs,
		state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckDedicated),
		state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckSharedOrDedicatedNUMABinding))

	general.Infof("[checkNonBindingShareCoresCpuResource] node cpu allocated: %d, allocatable: %d", shareCoresAllocatedInt, pooledCPUs.Size())
	if shareCoresAllocatedInt > pooledCPUs.Size() {
		general.Warningf("[checkNonBindingShareCoresCpuResource] no enough cpu resource for non-binding share cores pod: %s/%s, container: %s (request: %.02f, node allocated: %d, node allocatable: %d)",
			req.PodNamespace, req.PodName, req.ContainerName, reqFloat64, shareCoresAllocatedInt, pooledCPUs.Size())
		return false, nil
	}

	general.InfoS("checkNonBindingShareCoresCpuResource cpu successfully",
		"podNamespace", req.PodNamespace,
		"podName", req.PodName,
		"containerName", req.ContainerName,
		"request", reqFloat64)

	return true, nil
}

func (p *DynamicPolicy) applySidecarAllocationInfoFromMainContainer(sidecarAllocationInfo, mainAllocationInfo *state.AllocationInfo) bool {
	changed := false
	if sidecarAllocationInfo.OwnerPoolName != mainAllocationInfo.OwnerPoolName ||
		!sidecarAllocationInfo.AllocationResult.Equals(mainAllocationInfo.AllocationResult) ||
		!sidecarAllocationInfo.OriginalAllocationResult.Equals(mainAllocationInfo.OriginalAllocationResult) ||
		!state.CheckAllocationInfoTopologyAwareAssignments(sidecarAllocationInfo, mainAllocationInfo) ||
		!state.CheckAllocationInfoOriginTopologyAwareAssignments(sidecarAllocationInfo, mainAllocationInfo) {

		sidecarAllocationInfo.OwnerPoolName = mainAllocationInfo.OwnerPoolName
		sidecarAllocationInfo.AllocationResult = mainAllocationInfo.AllocationResult.Clone()
		sidecarAllocationInfo.OriginalAllocationResult = mainAllocationInfo.OriginalAllocationResult.Clone()
		sidecarAllocationInfo.TopologyAwareAssignments = machine.DeepcopyCPUAssignment(mainAllocationInfo.TopologyAwareAssignments)
		sidecarAllocationInfo.OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(mainAllocationInfo.OriginalTopologyAwareAssignments)

		changed = true
	}

	// Copy annotations from main container
	for key, value := range mainAllocationInfo.Annotations {
		if sidecarAllocationInfo.Annotations[key] != value {
			sidecarAllocationInfo.Annotations[key] = value
			changed = true
		}
	}

	request := p.getContainerRequestedCores(sidecarAllocationInfo)
	if sidecarAllocationInfo.RequestQuantity != request {
		sidecarAllocationInfo.RequestQuantity = request
		changed = true
	}

	return changed
}

// RegisterAllocationHook registers a hook that is called before allocation info is updated.
// It is concurrency-safe.
func (p *DynamicPolicy) RegisterAllocationHook(hook AllocationHook) {
	p.Lock()
	defer p.Unlock()
	p.allocationHooks = append(p.allocationHooks, hook)
}

// invokeAllocationHooks triggers all registered allocation hooks.
// Note: This method must be called with the lock held by the caller if concurrency protection is needed.
// We avoid internal locking here to prevent potential deadlocks when called from methods that already hold the lock.
func (p *DynamicPolicy) invokeAllocationHooks(oldAllocationInfo, newAllocationInfo *state.AllocationInfo) error {
	for _, hook := range p.allocationHooks {
		if err := hook(oldAllocationInfo, newAllocationInfo); err != nil {
			return err
		}
	}
	return nil
}

// invokeAllocationHooksForPodEntries triggers allocation hooks for non-pool containers before committing to state.
// Note: This method must be called with the lock held by the caller to ensure state consistency
// and avoid deadlocks due to nested locking.
func (p *DynamicPolicy) invokeAllocationHooksForPodEntries(curEntries, newEntries state.PodEntries) error {
	if len(p.allocationHooks) == 0 {
		return nil
	}

	for podUID, containerEntries := range newEntries {
		if containerEntries.IsPoolEntry() {
			continue
		}
		for containerName, newAllocationInfo := range containerEntries {
			var oldAllocationInfo *state.AllocationInfo
			// retrieve old allocation info from curEntries directly to avoid the overhead
			// of GetAllocationInfo which involves Clone() operations.
			if curContainerEntries, ok := curEntries[podUID]; ok {
				oldAllocationInfo = curContainerEntries[containerName]
			}
			if err := p.invokeAllocationHooks(oldAllocationInfo, newAllocationInfo); err != nil {
				return fmt.Errorf("invokeAllocationHooks failed for pod: %s, container: %s: %v", podUID, containerName, err)
			}
		}
	}
	return nil
}

// updateAllocationInfo wraps state.SetAllocationInfo with hook execution.
// If no hooks are registered, it avoids the overhead of retrieving the old allocation info.
func (p *DynamicPolicy) updateAllocationInfo(podUID, containerName string, oldAllocationInfo, allocationInfo *state.AllocationInfo, persist bool) error {
	if len(p.allocationHooks) > 0 {
		if oldAllocationInfo == nil {
			oldAllocationInfo = p.state.GetAllocationInfo(podUID, containerName)
		}
		if err := p.invokeAllocationHooks(oldAllocationInfo, allocationInfo); err != nil {
			return err
		}
	}

	p.state.SetAllocationInfo(podUID, containerName, allocationInfo, persist)
	return nil
}

// shouldBypassCPUSetAdjustment reports whether response cpuset backfill should
// be skipped for shared_cores, reclaimed_cores and system_cores pods.
func (p *DynamicPolicy) shouldBypassCPUSetAdjustment() bool {
	if p.dynamicConfig == nil {
		return false
	}
	dyn := p.dynamicConfig.GetDynamicConfiguration()
	return dyn != nil && dyn.EnableBypassCPUSetAdjustment
}

func (p *DynamicPolicy) shouldBypassCPUSetAdjustmentForAllocation(allocationInfo *state.AllocationInfo) bool {
	return p.shouldBypassCPUSetAdjustment() &&
		allocationInfo != nil &&
		(allocationInfo.CheckShared() || allocationInfo.CheckReclaimed() || allocationInfo.CheckSystem())
}

// clearCPUSetInAllocation clears the cpuset string on every entry of a
// *pluginapi.ResourceAllocation in place, leaving TopologyAssignments,
// AllocatedQuantity, ResourceHints and Annotations untouched. It is a no-op
// when the input is nil.
func clearCPUSetInAllocation(alloc *pluginapi.ResourceAllocation) {
	if alloc == nil {
		return
	}
	info := alloc.ResourceAllocation[string(v1.ResourceCPU)]
	if info != nil {
		info.AllocationResult = ""
	}
}

func (p *DynamicPolicy) clearCPUSetInAllocationResponseIfNeeded(resp *pluginapi.ResourceAllocationResponse, allocationInfo *state.AllocationInfo) {
	if resp == nil || !p.shouldBypassCPUSetAdjustmentForAllocation(allocationInfo) {
		return
	}
	clearCPUSetInAllocation(resp.AllocationResult)
}
