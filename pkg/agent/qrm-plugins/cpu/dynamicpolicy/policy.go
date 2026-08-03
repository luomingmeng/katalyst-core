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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	v1 "k8s.io/api/core/v1"
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
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer/policy"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer/registry"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/irqtuner"
	irqtuingcontroller "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/irqtuner/controller"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	cpuutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/util"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/agent/utilcomponent/featuregatenegotiation"
	"github.com/kubewharf/katalyst-core/pkg/agent/utilcomponent/periodicalhandler"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
	"github.com/kubewharf/katalyst-core/pkg/config/generic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/resourcepackage"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	cgroupclient "github.com/kubewharf/katalyst-core/pkg/util/cgroup/client"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/kubewharf/katalyst-core/pkg/util/metric"
	"github.com/kubewharf/katalyst-core/pkg/util/process"
	"github.com/kubewharf/katalyst-core/pkg/util/reclaim"
	"github.com/kubewharf/katalyst-core/pkg/util/timemonitor"
)

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

	healthCheckTolerationTimes = 3
)

var AccompanyResourceRegistry = accompanyresource.NewRegistry()

// AllocationHook is a hook function which can be registered and called when allocationInfo changes.
// It is designed to intercept state updates and perform actions like injecting or updating annotations
// (e.g., NUMA topology information) based on the differences between old and new allocation info.
type AllocationHook func(oldAllocationInfo, newAllocationInfo *state.AllocationInfo) error

// DynamicPolicy is the policy that's used by default;
// it will consider the dynamic running information to calculate
// and adjust resource requirements and configurations
type DynamicPolicy struct {
	sync.RWMutex
	pluginapi.UnimplementedResourcePluginServer

	name        string
	stopCh      chan struct{}
	started     bool
	lifecycleMu sync.Mutex

	lifecycleState policyLifecycleState
	lifecycleErr   error

	startedComponentStoppers []policyComponentStopper

	emitter     metrics.MetricEmitter
	metaServer  *metaserver.MetaServer
	machineInfo *machine.KatalystMachineInfo

	advisorClient advisorapi.CPUAdvisorClient
	advisorConn   *grpc.ClientConn
	advisorapi.UnimplementedCPUPluginServer
	advisorMonitor     *timemonitor.TimeMonitor
	featureGateManager featuregatenegotiation.FeatureGateManager

	advisorToken     uint64
	inMemoryRevision uint64

	advisorPostCommitMu     sync.Mutex
	advisorPostCommitOutbox *advisorPostCommitOutbox

	advisorCgroupPostCommitMu     sync.Mutex
	advisorCgroupPostCommitOutbox *advisorCgroupPostCommitOutbox

	state           state.State
	cgroupClient    cgroupclient.CgroupClient
	residualHitMap  map[string]int64
	hintHandlers    map[string]util.HintHandler
	allocationHooks []AllocationHook

	cpuPressureEviction       agent.Component
	cpuPressureEvictionCancel context.CancelFunc

	resourcePackageManager *resourcepackage.CachedResourcePackageManager

	irqTuner           irqtuner.Tuner
	cpuSetMaterializer cpusetmaterializer.Materializer

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
	var cpuSetMaterializer cpusetmaterializer.Materializer
	var dynamicConf *dynamicconfig.Configuration
	if conf.DynamicAgentConfiguration != nil {
		dynamicConf = conf.DynamicAgentConfiguration.GetDynamicConfiguration()
	}
	if dynamicConf != nil && dynamicConf.AdminQoSConfiguration != nil &&
		dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration != nil &&
		dynamicConf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.Enable {
		manager, err := bulkhead.NewManager(conf, bulkhead.RuntimeDependencies{
			DynamicConf: conf.DynamicAgentConfiguration,
			Emitter:     wrappedEmitter,
			MetaServer:  agentCtx.MetaServer,
			Topology:    agentCtx.CPUTopology,
		})
		if err != nil {
			return false, agent.ComponentStub{}, fmt.Errorf("dynamic policy init bulkhead manager failed with error: %v", err)
		}
		cpuSetMaterializer = manager
	}

	// since the reservedCPUs won't influence stateImpl directly.
	// so we don't modify stateImpl with reservedCPUs here.
	// for those pods have already been allocated reservedCPUs,
	// we won't touch them and wait them to be deleted the next update.
	policyImplement := &DynamicPolicy{
		name:           fmt.Sprintf("%s_%s", agentName, cpuconsts.CPUResourcePluginPolicyNameDynamic),
		stopCh:         make(chan struct{}),
		lifecycleState: policyLifecycleRecovering,

		machineInfo: agentCtx.KatalystMachineInfo,
		emitter:     wrappedEmitter,
		metaServer:  agentCtx.MetaServer,

		resourcePackageManager: resourcepackage.NewCachedResourcePackageManager(agentCtx.MetaServer.ResourcePackageManager),

		state:          stateImpl,
		cgroupClient:   cgroupclient.NewCgroupClient(),
		residualHitMap: make(map[string]int64),

		featureGateManager: featuregatenegotiation.NewFeatureGateManager(conf),

		cpuPressureEviction: cpuPressureEviction,
		cpuSetMaterializer:  cpuSetMaterializer,

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
		transitionPeriod:                30 * time.Second,
		reservedReclaimedCPUsSize:       getReservedReclaimedCPUsSize(agentCtx.KatalystMachineInfo.NumNUMANodes),
		reclaimConsumersForKCNR:         conf.ReclaimConsumersForKCNR,
	}
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

	// register hint providers for pods with different QoS level
	policyImplement.hintHandlers = map[string]util.HintHandler{
		consts.PodAnnotationQoSLevelSharedCores:    policyImplement.sharedCoresHintHandler,
		consts.PodAnnotationQoSLevelDedicatedCores: policyImplement.dedicatedCoresHintHandler,
		consts.PodAnnotationQoSLevelReclaimedCores: policyImplement.reclaimedCoresHintHandler,
		consts.PodAnnotationQoSLevelSystemCores:    policyImplement.systemCoresHintHandler,
	}

	if err := policyImplement.bootstrapPools(context.Background()); err != nil {
		return false, agent.ComponentStub{}, fmt.Errorf("bootstrap cpu pools failed with error: %v", err)
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

func getReservedReclaimedCPUsSize(numNUMANodes int) int {
	return general.Max(reservedReclaimedCPUsSize, numNUMANodes*2)
}

func getReservedReclaimedCPUsSizePerNUMA(total int, numaIDs []int) map[int]int {
	distribution := machine.GetCoreNumReservedForReclaim(total, len(numaIDs))
	result := make(map[int]int, len(numaIDs))
	for index, numaID := range numaIDs {
		result[numaID] = distribution[index]
	}
	return result
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

func (p *DynamicPolicy) Start() (err error) {
	general.Infof("called")
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	p.Lock()
	if p.started {
		if p.lifecycleState == policyLifecycleReady {
			general.Infof("is already started")
			p.Unlock()
			return nil
		}
		state := p.lifecycleState.String()
		p.Unlock()
		return fmt.Errorf("cpu policy start already in progress: state=%s", state)
	}
	p.started = true
	p.stopCh = make(chan struct{})
	p.startedComponentStoppers = []policyComponentStopper{{
		name: "runtime",
		stop: func() error {
			return stopPolicyChannel(p.stopCh)
		},
	}}
	p.lifecycleState = policyLifecycleRecovering
	p.lifecycleErr = nil
	p.Unlock()

	defer func() {
		if err != nil {
			if stopErr := p.stopStartedComponents(); stopErr != nil {
				general.ErrorS(stopErr, "failed to roll back cpu policy startup")
			}
		}
		p.Lock()
		defer p.Unlock()
		if err == nil {
			p.lifecycleState = policyLifecycleReady
			p.lifecycleErr = nil
			return
		}
		p.lifecycleState = policyLifecycleBlocked
		p.lifecycleErr = err
		p.started = false
	}()

	if err = p.recoverCommittedTarget(context.Background()); err != nil {
		return err
	}

	if p.irqTuner != nil {
		irqStopCh := make(chan struct{})
		go p.irqTuner.Run(irqStopCh)
		p.recordStartedComponent(policyComponentStopper{
			name: "irq",
			stop: func() error {
				_ = stopPolicyChannel(irqStopCh)
				p.irqTuner.Stop()
				return nil
			},
		})
	}

	go wait.Until(func() {
		_ = p.emitter.StoreInt64(util.MetricNameHeartBeat, 1, metrics.MetricTypeNameRaw)
	}, time.Second*30, p.stopCh)

	err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.ClearResidualState, general.HealthzCheckStateNotReady,
		qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.clearResidualState, stateCheckPeriod, healthCheckTolerationTimes)
	if err != nil {
		return fmt.Errorf("start %v failed: %w", cpuconsts.ClearResidualState, err)
	}
	p.recordStartedComponent(policyComponentStopper{
		name: "periodical",
		stop: func() error {
			periodicalhandler.StopHandlersByGroup(qrm.QRMCPUPluginPeriodicalHandlerGroupName)
			return nil
		},
	})

	err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.CheckCPUSet, general.HealthzCheckStateNotReady,
		qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.checkCPUSet, cpusetCheckPeriod, healthCheckTolerationTimes)
	if err != nil {
		return fmt.Errorf("start %v failed: %w", cpuconsts.CheckCPUSet, err)
	}

	err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.SyncSystemExclusivePool, general.HealthzCheckStateNotReady,
		qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.syncSystemExclusivePool, syncSystemExclusivePoolPeriod, healthCheckTolerationTimes)
	if err != nil {
		return fmt.Errorf("start %v failed: %w", cpuconsts.SyncSystemExclusivePool, err)
	}

	err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.SyncBulkhead, general.HealthzCheckStateNotReady,
		qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.runBulkheadPeriodicalHandlers, syncBulkheadPeriod, healthCheckTolerationTimes)
	if err != nil {
		return fmt.Errorf("start %v failed: %w", cpuconsts.SyncBulkhead, err)
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
			return fmt.Errorf("start %v failed: %w", cpuconsts.SyncCPUIdle, err)
		}
	}

	// start cpu burst sync if needed
	if p.enableCPUBurst {
		general.Infof("cpu burst is enabled")

		err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.SyncCPUBurst, general.HealthzCheckStateNotReady,
			qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.syncCPUBurst, syncCPUBurstPeriod, healthCheckTolerationTimes)
		if err != nil {
			return fmt.Errorf("start %v failed: %w", cpuconsts.SyncCPUBurst, err)
		}
	}

	if p.conf.CPUQRMPluginConfig.EnableCPUWeight {
		general.Infof("cpu weight is enabled")

		err = periodicalhandler.RegisterPeriodicalHandlerWithHealthz(cpuconsts.SyncCPUWeight, general.HealthzCheckStateNotReady,
			qrm.QRMCPUPluginPeriodicalHandlerGroupName, p.syncCPUWeight, syncCPUWeightPeriod, healthCheckTolerationTimes)
		if err != nil {
			return fmt.Errorf("start %v failed: %w", cpuconsts.SyncCPUWeight, err)
		}
	}

	// start cpu-pressure eviction plugin if needed
	if p.cpuPressureEviction != nil {
		p.Lock()
		if p.started {
			var ctx context.Context
			ctx, p.cpuPressureEvictionCancel = context.WithCancel(context.Background())
			go p.cpuPressureEviction.Run(ctx)
			cancel := p.cpuPressureEvictionCancel
			p.startedComponentStoppers = append(p.startedComponentStoppers, policyComponentStopper{
				name: "cpu-pressure-eviction",
				stop: func() error {
					cancel()
					return nil
				},
			})
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

	advisorStopCh := make(chan struct{})
	err = p.initAdvisorClientConn()
	if err != nil {
		general.Errorf("initAdvisorClientConn failed with error: %v", err)
		return
	}
	advisorConn := p.advisorConn
	p.recordStartedComponent(policyComponentStopper{
		name: "advisor",
		stop: func() error {
			_ = stopPolicyChannel(advisorStopCh)
			if advisorConn != nil {
				return advisorConn.Close()
			}
			return nil
		},
	})
	p.startAdvisorPostCommitWorker()
	p.recordStartedComponent(policyComponentStopper{
		name: "advisor-post-commit-outboxes",
		stop: func() error {
			p.stopAdvisorPostCommitWorker()
			return nil
		},
	})

	p.advisorMonitor, err = timemonitor.NewTimeMonitor(cpuAdvisorHealthMonitorName, cpuAdvisorHealthMonitorInterval,
		cpuAdvisorUnhealthyThreshold, cpuAdvisorHealthyThreshold,
		util.MetricNameAdvisorUnhealthy, p.emitter, cpuAdvisorHealthyCount, true)
	if err != nil {
		general.Errorf("initialize cpu advisor monitor failed with error: %v", err)
		return
	}
	go p.advisorMonitor.Run(advisorStopCh)

	go wait.BackoffUntil(func() { p.serveForAdvisor(advisorStopCh) }, wait.NewExponentialBackoffManager(
		800*time.Millisecond, 30*time.Second, 2*time.Minute, 2.0, 0, &clock.RealClock{}), true, advisorStopCh)

	communicateWithCPUAdvisorServer := func() {
		general.Infof("waiting cpu plugin checkpoint server serving confirmation")
		if conn, err := process.Dial(p.cpuPluginSocketAbsPath, 5*time.Second); err != nil {
			general.Errorf("dial check at socket: %s failed with err: %v", p.cpuPluginSocketAbsPath, err)
			return
		} else {
			_ = conn.Close()
		}
		general.Infof("cpu plugin checkpoint server serving confirmed")

		p.getAdviceFromAdvisorLoop(advisorStopCh)
		select {
		case <-advisorStopCh:
			// stopCh closed, no need to fall back to ListAndWatch.
			return
		default:
		}

		if p.isRampUpReclaimHardPartitionEnabled() {
			general.Errorf("advisor GetAdvice is required when ramp-up reclaim hard partition is enabled; skip legacy ListAndWatch fallback")
			return
		}

		general.Infof("advisor does not implement GetAdvice, fall back to ListAndWatch")

		if err := p.pushCPUAdvisor(); err != nil {
			general.Errorf("sync existing containers to cpu advisor failed with error: %v", err)
			return
		}
		general.Infof("sync existing containers to cpu advisor successfully")

		// call lw of CPUAdvisorServer and do allocation
		if err := p.lwCPUAdvisorServer(advisorStopCh); err != nil {
			general.Errorf("lwCPUAdvisorServer failed with error: %v", err)
		} else {
			general.Infof("lwCPUAdvisorServer finished")
		}
	}

	go wait.BackoffUntil(communicateWithCPUAdvisorServer, wait.NewExponentialBackoffManager(800*time.Millisecond,
		30*time.Second, 2*time.Minute, 2.0, 0, &clock.RealClock{}), true, advisorStopCh)

	resourcePackageStopCh := make(chan struct{})
	err = p.resourcePackageManager.Run(resourcePackageStopCh)
	if err != nil {
		return fmt.Errorf("resourcePackageManager.Run failed with error: %v", err)
	}
	p.recordStartedComponent(policyComponentStopper{
		name: "resource-package",
		stop: func() error {
			return stopPolicyChannel(resourcePackageStopCh)
		},
	})

	p.syncResourcePackagePinnedCPUSet()
	go wait.Until(p.syncResourcePackagePinnedCPUSet, 30*time.Second, resourcePackageStopCh)

	sharedOptimizerStopCh := make(chan struct{})
	err = p.sharedCoresNUMABindingHintOptimizer.Run(sharedOptimizerStopCh)
	if err != nil {
		return fmt.Errorf("sharedCoresNUMABindingHintOptimizer.Run failed with error: %v", err)
	}
	p.recordStartedComponent(policyComponentStopper{
		name: "shared-hint-optimizer",
		stop: func() error {
			return stopPolicyChannel(sharedOptimizerStopCh)
		},
	})

	dedicatedOptimizerStopCh := make(chan struct{})
	err = p.dedicatedCoresNUMABindingHintOptimizer.Run(dedicatedOptimizerStopCh)
	if err != nil {
		return fmt.Errorf("dedicatedCoresNUMABindingHintOptimizer.Run failed with error: %v", err)
	}
	p.recordStartedComponent(policyComponentStopper{
		name: "dedicated-hint-optimizer",
		stop: func() error {
			return stopPolicyChannel(dedicatedOptimizerStopCh)
		},
	})

	return nil
}

func (p *DynamicPolicy) Stop() error {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	p.Lock()
	if !p.started {
		p.Unlock()
		general.Warningf("already stopped")
		return nil
	}
	p.started = false
	if p.lifecycleState != policyLifecycleBlocked {
		p.lifecycleState = policyLifecycleRecovering
		p.lifecycleErr = nil
	}
	p.Unlock()

	stopErr := p.stopStartedComponents()

	general.Infof("stopped")
	return stopErr
}

// GetResourcesAllocation returns allocation results of corresponding resources
func (p *DynamicPolicy) GetResourcesAllocation(ctx context.Context,
	req *pluginapi.GetResourcesAllocationRequest,
) (*pluginapi.GetResourcesAllocationResponse, error) {
	if err := p.requireReady(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, fmt.Errorf("GetResourcesAllocation got nil req")
	}

	var resp *pluginapi.GetResourcesAllocationResponse
	err := p.transact(ctx, func(base *state.TargetState) (*state.TargetState, error) {
		editor := newTargetMutationEditor(base)
		var planErr error
		resp, planErr = p.getResourcesAllocationOnOwnedTarget(req, editor)
		return editor.target, planErr
	})
	return resp, err
}

func (p *DynamicPolicy) getResourcesAllocationOnOwnedTarget(
	req *pluginapi.GetResourcesAllocationRequest,
	editor *targetMutationEditor,
) (*pluginapi.GetResourcesAllocationResponse, error) {
	general.Infof("called")
	target := editor.target
	podEntries := target.GetPodEntries()
	machineState := target.GetMachineState()

	// rumpUpPooledCPUs is the total available cpu cores minus those that are reserved
	rumpUpPooledCPUs := machineState.GetFilteredAvailableCPUSet(p.reservedCPUs,
		func(ai *state.AllocationInfo) bool {
			return ai.CheckDedicated() || ai.CheckSharedNUMABinding()
		},
		state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckDedicatedNUMABinding))
	if p.requiresReclaimDisjoint(target.GetAllowSharedCoresOverlapReclaimedCores()) {
		if reclaimCPUs, reclaimErr := podEntries.GetCPUSetForPool(commonstate.PoolNameReclaim); reclaimErr == nil {
			rumpUpPooledCPUs = rumpUpPooledCPUs.Difference(reclaimCPUs)
		}
	}
	rumpUpPooledCPUsTopologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, rumpUpPooledCPUs)
	if err != nil {
		return nil, fmt.Errorf("GetNumaAwareAssignments err: %v", err)
	}

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
					if err := p.updateAllocationInfoOnTarget(
						podUID, containerName, originAllocationInfo, allocationInfo, true, target); err != nil {
						general.Errorf("updateAllocationInfo failed for pod: %s/%s, container: %s: %v",
							allocationInfo.PodNamespace, allocationInfo.PodName, containerName, err)
						continue
					}
					needUpdateMachineState = true
				}
			}

			initTs, tsErr := time.Parse(util.QRMTimeFormat, allocationInfo.InitTimestamp)
			if tsErr != nil {
				if allocationInfo.CheckShared() && !allocationInfo.CheckNUMABinding() {
					general.Errorf("pod: %s/%s, container: %s init timestamp parsed failed with error: %v, re-ramp-up it",
						allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName, tsErr)

					clonedPooledCPUs := rumpUpPooledCPUs.Clone()
					clonedPooledCPUsTopologyAwareAssignments := machine.DeepcopyCPUAssignment(rumpUpPooledCPUsTopologyAwareAssignments)

					allocationInfo.AllocationResult = clonedPooledCPUs
					allocationInfo.OriginalAllocationResult = clonedPooledCPUs
					allocationInfo.TopologyAwareAssignments = clonedPooledCPUsTopologyAwareAssignments
					allocationInfo.OriginalTopologyAwareAssignments = clonedPooledCPUsTopologyAwareAssignments
					// fill OwnerPoolName with empty string when ramping up
					allocationInfo.OwnerPoolName = commonstate.EmptyOwnerPoolName
					allocationInfo.RampUp = true
				}

				allocationInfo.InitTimestamp = time.Now().Format(util.QRMTimeFormat)
				if err := p.updateAllocationInfoOnTarget(
					podUID, containerName, originAllocationInfo, allocationInfo, true, target); err != nil {
					general.Errorf("updateAllocationInfo failed for pod: %s/%s, container: %s: %v",
						allocationInfo.PodNamespace, allocationInfo.PodName, containerName, err)
				}
			} else if allocationInfo.RampUp && time.Now().After(initTs.Add(p.transitionPeriod)) {
				if p.isRampUpReclaimHardPartitionEnabled() {
					// Expiry only changes the advisor phase in hard-partition mode. The
					// live reclaim pool remains the target until a stable candidate is
					// committed and bulkhead converges cgroups toward that committed state.
					continue
				}
				allocationInfo.RampUp = false
				if err := p.updateAllocationInfoOnTarget(
					podUID, containerName, originAllocationInfo, allocationInfo, true, target); err != nil {
					general.Errorf("updateAllocationInfo failed for pod: %s/%s, container: %s: %v",
						allocationInfo.PodNamespace, allocationInfo.PodName, containerName, err)
					continue
				}

				if allocationInfo.CheckShared() {
					allocationInfosJustFinishRampUp = append(allocationInfosJustFinishRampUp, allocationInfo)
				}
			}

		}
	}

	if len(allocationInfosJustFinishRampUp) > 0 {
		if err = p.putAllocationsAndAdjustAllocationEntriesResizeAwareOnTarget(
			nil, allocationInfosJustFinishRampUp, true, false, true, target); err != nil {
			// not influencing return response to kubelet when putAllocationsAndAdjustAllocationEntries failed
			general.Errorf("putAllocationsAndAdjustAllocationEntries failed with error: %v", err)
		}
	} else if needUpdateMachineState {
		// NOTE: we only need update machine state when putAllocationsAndAdjustAllocationEntries is skipped,
		// because putAllocationsAndAdjustAllocationEntries will update machine state.
		general.Infof("GetResourcesAllocation update machine state")
		podEntries = target.GetPodEntries()
		updatedMachineState, err := generateMachineStateFromPodEntries(p.machineInfo.CPUTopology, podEntries, machineState)
		if err != nil {
			general.Errorf("GetResourcesAllocation GenerateMachineStateFromPodEntries failed with error: %v", err)
			return nil, fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
		}
		target.MachineState = updatedMachineState.Clone()
	}

	podEntries = target.GetPodEntries()
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

	var summedPct float64
	for _, name := range consumerNames {
		pct := reclaim.GetReclaimedPercentage(p.dynamicConfig.GetDynamicConfiguration(), name)
		summedPct += pct
	}
	if summedPct > 100 {
		summedPct = 100
	}

	numaHeadroom := p.state.GetNUMAHeadroom()

	topologyAwareList := make([]*pluginapi.TopologyAwareQuantity, 0, len(numaNodes))
	for _, numaNode := range numaNodes {
		scaled := numaHeadroom[numaNode] * 1000 * summedPct / 100
		topologyAwareList = append(topologyAwareList, &pluginapi.TopologyAwareQuantity{
			ResourceValue: scaled,
			Node:          uint64(numaNode),
		})
	}

	var totalHeadroom float64
	for _, v := range numaHeadroom {
		totalHeadroom += v
	}
	aggregated := totalHeadroom * 1000 * summedPct / 100

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
	p.RLock()
	defer p.RUnlock()
	if err := p.requireReadyLocked(); err != nil {
		return nil, err
	}
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
	defer func() {
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
	if err := p.requireReady(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, fmt.Errorf("allocate got nil req")
	}

	plannedReq, ok := proto.Clone(req).(*pluginapi.ResourceRequest)
	if !ok || plannedReq == nil {
		return nil, fmt.Errorf("deep copy allocate request failed")
	}
	existReallocAnno, isReallocation := util.IsReallocation(plannedReq.Annotations)
	var observation allocationPlanObservation
	var advisorAdd *advisorsvc.ContainerMetadata
	startTime := time.Now()
	respErr = p.transactWithPostCommit(ctx, func(base *state.TargetState) (*state.TargetState, error) {
		editor := newTargetMutationEditor(base)
		resp, observation, respErr = p.planAllocationOnOwnedTarget(ctx, plannedReq, editor)
		if respErr == nil && p.enableCPUAdvisor && p.advisorClient != nil &&
			plannedReq.ContainerType != pluginapi.ContainerType_INIT {
			allocationInfo := editor.target.GetAllocationInfo(plannedReq.PodUid, plannedReq.ContainerName)
			qosLevel := ""
			if allocationInfo != nil {
				qosLevel = allocationInfo.QoSLevel
			}
			reqInt, reqFloat64, quantityErr := util.GetQuantityFromResourceReq(plannedReq)
			if quantityErr != nil {
				return nil, quantityErr
			}
			advisorAdd = buildAdvisorAddMetadata(plannedReq, qosLevel, reqInt, reqFloat64)
		}
		return editor.target, respErr
	}, func() {
		if advisorAdd != nil {
			p.enqueueAdvisorAdd(advisorAdd)
		}
	})
	if respErr != nil {
		general.ErrorS(respErr, "Allocate failed",
			"duration", time.Since(startTime).String(),
			"podNamespace", plannedReq.PodNamespace,
			"podName", plannedReq.PodName,
			"containerName", plannedReq.ContainerName)
		p.reportOrphanContainerError(respErr)
		inplaceUpdateResizing := util.PodInplaceUpdateResizing(plannedReq)
		metricTags := []metrics.MetricTag{
			{Key: "error_message", Val: metric.MetricTagValueFormat(respErr)},
			{Key: util.MetricTagNameInplaceUpdateResizing, Val: strconv.FormatBool(inplaceUpdateResizing)},
		}
		if existReallocAnno {
			metricTags = append(metricTags, metrics.MetricTag{Key: "reallocation", Val: isReallocation})
		}
		_ = p.emitter.StoreInt64(util.MetricNameAllocateFailed, 1, metrics.MetricTypeNameRaw, metricTags...)
		return nil, respErr
	}
	general.InfoS("Allocate succeeded",
		"duration", time.Since(startTime).String(),
		"podNamespace", plannedReq.PodNamespace,
		"podName", plannedReq.PodName,
		"containerName", plannedReq.ContainerName)
	if observation.seededSharedPoolName != "" {
		_ = p.emitter.StoreInt64(util.MetricNameSharedCoresRampUpDisabledSeeded, 1,
			metrics.MetricTypeNameCount,
			metrics.MetricTag{Key: "poolName", Val: observation.seededSharedPoolName},
			metrics.MetricTag{Key: "overlap", Val: strconv.FormatBool(observation.overlap)},
		)
	}
	if err := AccompanyResourceRegistry.AllocateAccompanyResource(plannedReq, resp); err != nil {
		general.ErrorS(err, "post-commit accompany resource allocation failed",
			"podUID", plannedReq.PodUid, "containerName", plannedReq.ContainerName)
	}
	return resp, nil
}

type allocationPlanObservation struct {
	seededSharedPoolName string
	overlap              bool
}

func (p *DynamicPolicy) planAllocationOnOwnedTarget(
	ctx context.Context,
	req *pluginapi.ResourceRequest,
	editor *targetMutationEditor,
) (*pluginapi.ResourceAllocationResponse, allocationPlanObservation, error) {
	before := editor.target.Clone()
	resp, err := p.allocateOnOwnedTarget(ctx, req, editor)
	if err != nil {
		return resp, allocationPlanObservation{}, err
	}

	allocationInfo := editor.target.GetAllocationInfo(req.PodUid, req.ContainerName)
	if allocationInfo == nil || !allocationInfo.CheckShared() || allocationInfo.CheckNUMABinding() || allocationInfo.RampUp {
		return resp, allocationPlanObservation{}, nil
	}
	poolName := allocationInfo.GetSpecifiedPoolName()
	if before.GetAllocationInfo(poolName, commonstate.FakedContainerName) != nil ||
		editor.target.GetAllocationInfo(poolName, commonstate.FakedContainerName) == nil {
		return resp, allocationPlanObservation{}, nil
	}
	return resp, allocationPlanObservation{
		seededSharedPoolName: poolName,
		overlap:              editor.target.GetAllowSharedCoresOverlapReclaimedCores(),
	}, nil
}

func (p *DynamicPolicy) allocateOnOwnedTarget(ctx context.Context,
	req *pluginapi.ResourceRequest,
	editor *targetMutationEditor,
) (resp *pluginapi.ResourceAllocationResponse, respErr error) {
	// identify if the pod is a debug pod,
	// if so, apply specific strategy to it.
	// since GetKatalystQoSLevelFromResourceReq function will filter annotations,
	// we should do it before GetKatalystQoSLevelFromResourceReq.
	isDebugPod := util.IsDebugPod(req.Annotations, p.podDebugAnnoKeys)

	originalAnnotations := general.DeepCopyMap(req.Annotations)
	qosLevel, err := util.GetKatalystQoSLevelFromResourceReq(p.qosConfig, req, p.podAnnotationKeptKeys, p.podLabelKeptKeys)
	for key, value := range originalAnnotations {
		if !strings.Contains(key, "/") ||
			key == consts.PodAnnotationMemoryEnhancementNumaBinding ||
			key == consts.PodAnnotationMemoryEnhancementNumaExclusive ||
			key == consts.PodAnnotationCPUEnhancementNumaNumber {
			req.Annotations[key] = value
		}
	}
	if req.Hint != nil && len(req.Hint.Nodes) > 0 {
		req.Annotations[consts.PodAnnotationMemoryEnhancementNumaBinding] =
			consts.PodAnnotationMemoryEnhancementNumaBindingEnable
	}
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
	allocationInfo := editor.target.GetAllocationInfo(req.PodUid, req.ContainerName)
	if allocationInfo != nil && allocationInfo.OriginalAllocationResult.Size() >= reqInt && !util.PodInplaceUpdateResizing(req) {
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

	var handler util.AllocationHandler
	switch qosLevel {
	case consts.PodAnnotationQoSLevelSharedCores:
		handler = func(ctx context.Context, req *pluginapi.ResourceRequest, persist bool) (*pluginapi.ResourceAllocationResponse, error) {
			return p.sharedCoresAllocationHandlerOnTarget(ctx, req, persist, editor.target)
		}
	case consts.PodAnnotationQoSLevelDedicatedCores:
		handler = func(ctx context.Context, req *pluginapi.ResourceRequest, persist bool) (*pluginapi.ResourceAllocationResponse, error) {
			return p.dedicatedCoresAllocationHandlerOnTarget(ctx, req, persist, editor.target)
		}
	case consts.PodAnnotationQoSLevelReclaimedCores:
		handler = func(ctx context.Context, req *pluginapi.ResourceRequest, persist bool) (*pluginapi.ResourceAllocationResponse, error) {
			return p.reclaimedCoresAllocationHandlerOnTarget(ctx, req, persist, editor.target)
		}
	case consts.PodAnnotationQoSLevelSystemCores:
		handler = func(ctx context.Context, req *pluginapi.ResourceRequest, persist bool) (*pluginapi.ResourceAllocationResponse, error) {
			return p.systemCoresAllocationHandlerOnTarget(ctx, req, persist, editor.target)
		}
	}
	if handler == nil {
		return nil, fmt.Errorf("katalyst QoS level: %s is not supported yet", qosLevel)
	}
	return handler(ctx, req, false)
}

func buildAdvisorAddMetadata(
	req *pluginapi.ResourceRequest,
	qosLevel string,
	reqInt int,
	reqFloat64 float64,
) *advisorsvc.ContainerMetadata {
	return &advisorsvc.ContainerMetadata{
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
	}
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
	if err := p.requireReady(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, fmt.Errorf("RemovePod got nil req")
	}

	general.InfoS("called", "podUID", req.PodUid)
	startTime := time.Now()
	defer func() {
		general.InfoS("finished", "duration", time.Since(startTime).String(), "podUID", req.PodUid)
	}()

	err = p.transactWithPostCommit(ctx, func(base *state.TargetState) (*state.TargetState, error) {
		editor := newTargetMutationEditor(base)
		resp, err = p.removePodOnOwnedTarget(req, editor)
		return editor.target, err
	}, func() {
		if p.enableCPUAdvisor && p.advisorClient != nil {
			p.enqueueAdvisorRemove(req.PodUid)
		}
	})
	if err != nil {
		_ = p.emitter.StoreInt64(util.MetricNameRemovePodFailed, 1, metrics.MetricTypeNameRaw,
			metrics.MetricTag{Key: "error_message", Val: metric.MetricTagValueFormat(err)})
		general.ErrorS(err, "RemovePod failed", "podUID", req.PodUid)
		return nil, err
	}
	if releaseErr := AccompanyResourceRegistry.ReleaseAccompanyResource(req); releaseErr != nil {
		general.ErrorS(releaseErr, "post-commit accompany resource release failed", "podUID", req.PodUid)
	}
	return resp, nil
}

func (p *DynamicPolicy) removePodOnOwnedTarget(
	req *pluginapi.RemovePodRequest,
	editor *targetMutationEditor,
) (resp *pluginapi.RemovePodResponse, err error) {
	target := editor.target
	podEntries := target.GetPodEntries()
	if len(podEntries[req.PodUid]) == 0 {
		return &pluginapi.RemovePodResponse{}, nil
	}

	err = p.removePodFromTarget(req.PodUid, podEntries, target)
	if err != nil {
		return nil, err
	}

	_ = p.adjustAllocationEntriesOnTarget(podEntries, target.GetMachineState(), false, target)
	return &pluginapi.RemovePodResponse{}, nil
}

func (p *DynamicPolicy) removePodFromTarget(podUID string, podEntries state.PodEntries, target *state.TargetState) error {
	delete(podEntries, podUID)
	updatedMachineState, err := generateMachineStateFromPodEntries(
		p.machineInfo.CPUTopology, podEntries, target.GetMachineState())
	if err != nil {
		return fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
	}
	target.PodEntries = podEntries.Clone()
	target.MachineState = updatedMachineState.Clone()
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

func (p *DynamicPolicy) bootstrapPools(ctx context.Context) error {
	var reservedReclaimedCPUSet machine.CPUSet
	var reservedReclaimedAssignments map[int]machine.CPUSet
	err := p.transactBootstrap(ctx, func(target *state.TargetState) (*state.TargetState, error) {
		if err := p.cleanPoolsOnTarget(target); err != nil {
			return nil, fmt.Errorf("clean pools: %w", err)
		}
		if err := p.initReservePoolOnTarget(target); err != nil {
			return nil, fmt.Errorf("init reserve pool: %w", err)
		}
		var err error
		reservedReclaimedCPUSet, reservedReclaimedAssignments, err = p.initReclaimPoolOnTarget(target)
		if err != nil {
			return nil, fmt.Errorf("init reclaim pool: %w", err)
		}
		if p.conf.EnableIRQTuner {
			if err := p.initInterruptPoolOnTarget(target); err != nil {
				return nil, fmt.Errorf("init interrupt pool: %w", err)
			}
		}
		return target, nil
	})
	if err != nil {
		return err
	}
	p.reservedReclaimedCPUSet = reservedReclaimedCPUSet.Clone()
	p.reservedReclaimedTopologyAwareAssignments = machine.DeepcopyCPUAssignment(reservedReclaimedAssignments)
	return nil
}

func (p *DynamicPolicy) cleanPoolsOnTarget(target *state.TargetState) error {
	remainPools := make(map[string]bool)

	// walk through pod entries to put them into specified pool maps
	podEntries := target.GetPodEntries()
	for _, entries := range podEntries {
		if entries.IsPoolEntry() {
			continue
		}

		for containerName, allocationInfo := range entries {
			if allocationInfo == nil {
				general.Warningf("container %s allocation info is nil during cleanPools, skip it", containerName)
				continue
			}
			ownerPool := allocationInfo.GetOwnerPoolName()
			if ownerPool != commonstate.EmptyOwnerPoolName {
				remainPools[ownerPool] = true
			}
		}
	}

	// if pool exists in entries, but has no corresponding container, we need to delete it
	poolsToDelete := sets.NewString()
	for poolName, entries := range podEntries {
		if entries.IsPoolEntry() {
			// system pool is managed separately, should skip it
			if commonstate.IsSystemPool(poolName) {
				continue
			}
			if !remainPools[poolName] && !state.ResidentPools.Has(poolName) {
				poolsToDelete.Insert(poolName)
			}
		}
	}

	if poolsToDelete.Len() > 0 {
		for _, poolName := range poolsToDelete.UnsortedList() {
			delete(podEntries, poolName)
		}
		target.PodEntries = podEntries.Clone()
	}

	return nil
}

// initReservePool initializes reserve pool for system cores workload
func (p *DynamicPolicy) initReservePoolOnTarget(target *state.TargetState) error {
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
	putTargetAllocation(target, commonstate.PoolNameReserve, commonstate.FakedContainerName, curReserveAllocationInfo)

	return nil
}

// initReclaimPool initializes pools for reclaimed-cores.
// if this info already exists in state-file, just use it, otherwise calculate right away
func (p *DynamicPolicy) initReclaimPoolOnTarget(target *state.TargetState) (machine.CPUSet, map[int]machine.CPUSet, error) {
	// for reclaimed pool, we must make them exist when the node isn't in hybrid mode even if cause overlap
	allAvailableCPUs := p.machineInfo.CPUDetails.CPUs().Difference(p.reservedCPUs)
	reclaimedAllocationInfo := target.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	previousReclaimedCPUSet := machine.NewCPUSet()
	if reclaimedAllocationInfo != nil {
		previousReclaimedCPUSet = reclaimedAllocationInfo.AllocationResult
	}
	defaultReservedReclaimedCPUSet, err := p.selectReservedReclaimedCPUSet(allAvailableCPUs, previousReclaimedCPUSet)
	if err != nil {
		return machine.NewCPUSet(), nil, fmt.Errorf("select reserved reclaimed cpuset failed with error: %v", err)
	}

	defaultReservedTopologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, defaultReservedReclaimedCPUSet)
	if err != nil {
		return machine.NewCPUSet(), nil, fmt.Errorf("unable to calculate defaultReservedTopologyAwareAssignments for pool: %s, "+
			"result cpuset: %s, error: %v", commonstate.PoolNameReclaim, defaultReservedReclaimedCPUSet.String(), err)
	}

	if reclaimedAllocationInfo == nil {
		podEntries := target.GetPodEntries()
		noneResidentCPUs := podEntries.GetFilteredPoolsCPUSet(state.ResidentPools)

		machineState := target.GetMachineState()
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
			return machine.NewCPUSet(), nil, fmt.Errorf("takeByNUMABalance faild in initReclaimPool for %s and %s with error: %v",
				commonstate.PoolNameShare, commonstate.PoolNameReclaim, err)
		}

		// for residual pools, we must make them exist even if cause overlap
		// todo: noneResidentCPUs is the same as reservedCPUs, why should we do this?
		if reclaimedCPUSet.IsEmpty() {
			reclaimedCPUSet = defaultReservedReclaimedCPUSet.Clone()
		}

		topologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, reclaimedCPUSet)
		if err != nil {
			return machine.NewCPUSet(), nil, fmt.Errorf("unable to calculate topologyAwareAssignments for pool: %s, "+
				"result cpuset: %s, error: %v", commonstate.PoolNameReclaim, reclaimedCPUSet.String(), err)
		}

		curPoolAllocationInfo := &state.AllocationInfo{
			AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
			AllocationResult:                 reclaimedCPUSet.Clone(),
			OriginalAllocationResult:         reclaimedCPUSet.Clone(),
			TopologyAwareAssignments:         topologyAwareAssignments,
			OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(topologyAwareAssignments),
		}
		putTargetAllocation(target, commonstate.PoolNameReclaim, commonstate.FakedContainerName, curPoolAllocationInfo)
	}

	return defaultReservedReclaimedCPUSet, defaultReservedTopologyAwareAssignments, nil
}

func (p *DynamicPolicy) selectReservedReclaimedCPUSet(allAvailableCPUs, previousReclaimedCPUSet machine.CPUSet) (machine.CPUSet, error) {
	if p.machineInfo == nil || p.machineInfo.CPUTopology == nil {
		return machine.NewCPUSet(), fmt.Errorf("machine topology is nil")
	}

	previousReclaimedCPUSet = previousReclaimedCPUSet.Intersection(allAvailableCPUs)
	if previousReclaimedCPUSet.IsEmpty() {
		selected, _, err := calculator.TakeHTByNUMABalance(p.machineInfo, allAvailableCPUs, p.reservedReclaimedCPUsSize)
		if err != nil {
			return machine.NewCPUSet(), fmt.Errorf("take NUMA-balanced reserved reclaim CPUs failed: %v", err)
		}
		return selected, nil
	}

	numaIDs := p.machineInfo.CPUDetails.NUMANodes().ToSliceInt()
	if len(numaIDs) == 0 {
		return machine.NewCPUSet(), fmt.Errorf("machine has no NUMA nodes")
	}

	reservedPerNUMA := getReservedReclaimedCPUsSizePerNUMA(p.reservedReclaimedCPUsSize, numaIDs)
	selected := machine.NewCPUSet()
	for _, numaID := range numaIDs {
		target := reservedPerNUMA[numaID]
		if target <= 0 {
			continue
		}

		availableInNUMA := allAvailableCPUs.Intersection(p.machineInfo.CPUDetails.CPUsInNUMANodes(numaID))
		if availableInNUMA.Size() < target {
			return machine.NewCPUSet(), fmt.Errorf("insufficient CPUs for reserved reclaim on NUMA %d: requested %d, available %d",
				numaID, target, availableInNUMA.Size())
		}

		preferred := previousReclaimedCPUSet.Intersection(availableInNUMA)
		selectedInNUMA := preferred
		if preferred.Size() > target {
			var err error
			selectedInNUMA, err = calculator.TakeByTopology(p.machineInfo, preferred, target, true)
			if err != nil {
				return machine.NewCPUSet(), fmt.Errorf("take previous reclaim CPUs on NUMA %d failed: %v", numaID, err)
			}
		}

		remaining := target - selectedInNUMA.Size()
		if remaining > 0 {
			filled, err := calculator.TakeByTopology(p.machineInfo, availableInNUMA.Difference(selectedInNUMA), remaining, true)
			if err != nil {
				return machine.NewCPUSet(), fmt.Errorf("fill reserved reclaim CPUs on NUMA %d failed: %v", numaID, err)
			}
			selectedInNUMA = selectedInNUMA.Union(filled)
		}
		selected = selected.Union(selectedInNUMA)
	}

	if selected.Size() != p.reservedReclaimedCPUsSize {
		return machine.NewCPUSet(), fmt.Errorf("reserved reclaim CPU count mismatch: expected %d, got %d",
			p.reservedReclaimedCPUsSize, selected.Size())
	}
	return selected, nil
}

func (p *DynamicPolicy) initInterruptPoolOnTarget(target *state.TargetState) error {
	interruptAllocationInfo := target.GetAllocationInfo(commonstate.PoolNameInterrupt, commonstate.FakedContainerName)
	if interruptAllocationInfo == nil {
		allocationInfo := &state.AllocationInfo{
			AllocationMeta: commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameInterrupt),
		}
		putTargetAllocation(target, commonstate.PoolNameInterrupt, commonstate.FakedContainerName, allocationInfo)
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

func (p *DynamicPolicy) updateAllocationInfoOnTarget(
	podUID, containerName string,
	oldAllocationInfo, allocationInfo *state.AllocationInfo,
	persist bool,
	target *state.TargetState,
) error {
	if len(p.allocationHooks) > 0 {
		if oldAllocationInfo == nil {
			oldAllocationInfo = target.GetAllocationInfo(podUID, containerName)
		}
		if err := p.invokeAllocationHooks(oldAllocationInfo, allocationInfo); err != nil {
			return err
		}
	}
	putTargetAllocation(target, podUID, containerName, allocationInfo)
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
