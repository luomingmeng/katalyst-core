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

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/advisorsvc"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/memory/dynamicpolicy/memoryadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/reporter"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

const (
	memoryServerName           string = "memory-server"
	durationToWaitAddContainer        = time.Second * 30

	memoryServerLWHealthCheckName = "memory-server-lw"
)

type memoryServer struct {
	*baseServer
	hasListAndWatchLoop     atomic.Value
	headroomResourceManager reporter.HeadroomResourceManager
}

func NewMemoryServer(
	conf *config.Configuration,
	headroomResourceManager reporter.HeadroomResourceManager,
	metaCache metacache.MetaCache,
	metaServer *metaserver.MetaServer,
	advisor subResourceAdvisor,
	emitter metrics.MetricEmitter,
) (*memoryServer, error) {
	ms := &memoryServer{}
	ms.baseServer = newBaseServer(memoryServerName, conf, metaCache, metaServer, emitter, advisor, ms)
	ms.hasListAndWatchLoop.Store(false)
	ms.advisorSocketPath = conf.MemoryAdvisorSocketAbsPath
	ms.pluginSocketPath = conf.MemoryPluginSocketAbsPath
	ms.headroomResourceManager = headroomResourceManager
	ms.resourceRequestName = "MemoryRequest"
	return ms, nil
}

func (ms *memoryServer) createQRMClient() (advisorsvc.QRMServiceClient, io.Closer, error) {
	if !general.IsPathExists(ms.pluginSocketPath) {
		return nil, nil, fmt.Errorf("memory plugin socket path %s does not exist", ms.pluginSocketPath)
	}
	conn, err := ms.dial(ms.pluginSocketPath, ms.period)
	if err != nil {
		return nil, nil, fmt.Errorf("dial memory plugin socket failed: %w", err)
	}
	return advisorsvc.NewQRMServiceClient(conn), conn, nil
}

func (ms *memoryServer) RegisterAdvisorServer() {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	grpcServer := grpc.NewServer()
	advisorsvc.RegisterAdvisorServiceServer(grpcServer, ms)
	ms.grpcServer = grpcServer
}

func (ms *memoryServer) populateMetaCache(memoryPluginClient advisorsvc.QRMServiceClient) error {
	general.Infof("start to populate metaCache")
	resp, err := memoryPluginClient.ListContainers(context.TODO(), &advisorsvc.Empty{})
	if err != nil {
		if !general.IsUnimplementedError(err) {
			return fmt.Errorf("list containers failed: %w", err)
		}

		// If ListContainers RPC method is not implemented, we need to wait for the QRM plugin to call AddContainer to update the metaCache.
		// Actually, this does not guarantee that all the containers will be fully walked through.
		general.Infof("waiting %v for qrm plugin to call AddContainer", durationToWaitAddContainer.String())
		time.Sleep(durationToWaitAddContainer)
	} else {
		for _, container := range resp.Containers {
			if err := ms.addContainer(container); err != nil {
				return fmt.Errorf("add container %s/%s failed: %w", container.PodUid, container.ContainerName, err)
			}
			general.InfoS("add container", "container", container.String())
		}
	}

	return nil
}

func (ms *memoryServer) ListAndWatch(_ *advisorsvc.Empty, server advisorsvc.AdvisorService_ListAndWatchServer) error {
	_ = ms.emitter.StoreInt64(ms.genMetricsName(metricServerLWCalled), int64(ms.period.Seconds()), metrics.MetricTypeNameCount)

	if ms.hasListAndWatchLoop.Swap(true).(bool) {
		klog.Warningf("[qosaware-server-memory] another ListAndWatch loop is running")
		return fmt.Errorf("another ListAndWatch loop is running")
	}
	defer ms.hasListAndWatchLoop.Store(false)

	// list containers to make sure metaCache is populated before memory advisor updates.
	memoryPluginClient, conn, err := ms.createQRMClient()
	if err != nil {
		klog.Errorf("[qosaware-server-memory] create memory plugin client failed: %v", err)
		return fmt.Errorf("create memory plugin client failed: %w", err)
	}
	defer conn.Close()
	if err := ms.populateMetaCache(memoryPluginClient); err != nil {
		klog.Errorf("[qosaware-server-memory] populate metaCache failed: %v", err)
		return fmt.Errorf("populate metaCache failed: %w", err)
	}

	klog.Infof("[qosaware-server-memory] start to push memory advice")
	general.RegisterTemporaryHeartbeatCheck(memoryServerLWHealthCheckName, healthCheckTolerationDuration, general.HealthzCheckStateNotReady, healthCheckTolerationDuration)
	defer general.UnregisterTemporaryHeartbeatCheck(memoryServerLWHealthCheckName)

	timer := time.NewTimer(ms.period)
	defer timer.Stop()

	for {
		select {
		case <-server.Context().Done():
			klog.Infof("[qosaware-server-memory] lw stream server exited")
			return nil
		case <-ms.stopCh:
			klog.Infof("[qosaware-server-memory] lw stopped because %v stopped", ms.name)
			return nil
		case <-timer.C:
			klog.Infof("[qosaware-server-memory] trigger advisor update")
			if err := ms.getAndPushAdvice(server); err != nil {
				klog.Errorf("[qosaware-server-memory] get and push advice failed: %v", err)
				_ = general.UpdateHealthzStateByError(memoryServerLWHealthCheckName, err)
			} else {
				_ = general.UpdateHealthzStateByError(memoryServerLWHealthCheckName, nil)
			}
			timer.Reset(ms.period)
		}
	}
}

func (ms *memoryServer) getAndPushAdvice(server advisorsvc.AdvisorService_ListAndWatchServer) error {
	advisorRespRaw, err := ms.resourceAdvisor.UpdateAndGetAdvice()
	if err != nil {
		_ = ms.emitter.StoreInt64(ms.genMetricsName(metricServerAdvisorUpdateFailed), int64(ms.period.Seconds()), metrics.MetricTypeNameCount)
		return fmt.Errorf("get memory advice failed: %w", err)
	}
	advisorResp, ok := advisorRespRaw.(*types.InternalMemoryCalculationResult)
	if !ok {
		_ = ms.emitter.StoreInt64(ms.genMetricsName(metricServerAdvisorUpdateFailed), int64(ms.period.Seconds()), metrics.MetricTypeNameCount)
		return fmt.Errorf("get memory advice failed: invalid type %T", advisorRespRaw)
	}
	klog.Infof("[qosaware-server-memory] get memory advice: %v", general.ToString(advisorResp))

	lwResp := ms.assembleResponse(advisorResp)
	if err := server.Send(lwResp); err != nil {
		_ = ms.emitter.StoreInt64(ms.genMetricsName(metricServerLWSendResponseFailed), int64(ms.period.Seconds()), metrics.MetricTypeNameCount)
		return fmt.Errorf("send listWatch response failed: %w", err)
	}
	klog.Infof("[qosaware-server-memory] sent listWatch resp: %v", general.ToString(lwResp))
	_ = ms.emitter.StoreInt64(ms.genMetricsName(metricServerLWSendResponseSucceeded), int64(ms.period.Seconds()), metrics.MetricTypeNameCount)
	return nil
}

// assmble per-numa headroom
func (ms *memoryServer) assembleHeadroom() *advisorsvc.CalculationInfo {
	numaAllocatable, err := ms.headroomResourceManager.GetNumaAllocatable()
	if err != nil {
		general.ErrorS(err, "get numa allocatable failed")
		return nil
	}

	numaHeadroom := make(memoryadvisor.MemoryNUMAHeadroom)
	for numaID, res := range numaAllocatable {
		numaHeadroom[numaID] = res.Value()
	}
	data, err := json.Marshal(numaHeadroom)
	if err != nil {
		general.ErrorS(err, "marshal numa headroom failed")
		return nil
	}

	calculationResult := &advisorsvc.CalculationResult{
		Values: map[string]string{
			string(memoryadvisor.ControlKnobKeyMemoryNUMAHeadroom): string(data),
		},
	}

	return &advisorsvc.CalculationInfo{
		CgroupPath:        "",
		CalculationResult: calculationResult,
	}
}

func (ms *memoryServer) assembleResponse(result *types.InternalMemoryCalculationResult) *advisorsvc.ListAndWatchResponse {
	if result == nil {
		return nil
	}

	resp := advisorsvc.ListAndWatchResponse{
		PodEntries:   make(map[string]*advisorsvc.CalculationEntries),
		ExtraEntries: make([]*advisorsvc.CalculationInfo, 0),
	}

	for _, advice := range result.ContainerEntries {
		podEntry, ok := resp.PodEntries[advice.PodUID]
		if !ok {
			podEntry = &advisorsvc.CalculationEntries{
				ContainerEntries: map[string]*advisorsvc.CalculationInfo{},
			}
			resp.PodEntries[advice.PodUID] = podEntry
		}
		calculationInfo, ok := podEntry.ContainerEntries[advice.ContainerName]
		if !ok {
			calculationInfo = &advisorsvc.CalculationInfo{
				CalculationResult: &advisorsvc.CalculationResult{
					Values: make(map[string]string),
				},
			}
			podEntry.ContainerEntries[advice.ContainerName] = calculationInfo
		}
		for k, v := range advice.Values {
			calculationInfo.CalculationResult.Values[k] = v
		}
	}

	for _, advice := range result.ExtraEntries {
		found := false
		for _, entry := range resp.ExtraEntries {
			if advice.CgroupPath == entry.CgroupPath {
				found = true
				for k, v := range advice.Values {
					entry.CalculationResult.Values[k] = v
				}
				break
			}
		}
		if !found {
			calculationInfo := &advisorsvc.CalculationInfo{
				CgroupPath: advice.CgroupPath,
				CalculationResult: &advisorsvc.CalculationResult{
					Values: general.DeepCopyMap(advice.Values),
				},
			}
			resp.ExtraEntries = append(resp.ExtraEntries, calculationInfo)
		}
	}

	extraNumaHeadroom := ms.assembleHeadroom()
	if extraNumaHeadroom != nil {
		resp.ExtraEntries = append(resp.ExtraEntries, extraNumaHeadroom)
	}

	return &resp
}
