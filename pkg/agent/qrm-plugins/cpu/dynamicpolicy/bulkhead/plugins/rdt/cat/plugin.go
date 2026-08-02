/*
Copyright 2026 The Katalyst Authors.

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

package cat

import (
	"context"
	"errors"
	"fmt"
	"math/bits"

	apierrors "k8s.io/apimachinery/pkg/util/errors"

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	qrmresctrlmanager "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/resctrl"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	qrmresctrl "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/resctrl"
	"github.com/kubewharf/katalyst-core/pkg/util/external/rdt"
	resctrlutil "github.com/kubewharf/katalyst-core/pkg/util/resctrl"
)

const CATPluginName = "rdt_cat"

var _ bulkheadapi.Plugin = (*CATPlugin)(nil)

// ClosManager provides the explicit CLOS ownership boundary for CAT. It must
// never return a foreign CLOS merely because its name has a familiar prefix.
type ClosManager interface {
	ListCATManagedClos(context.Context) ([]qrmresctrlmanager.CPUListClos, error)
}

type CATPlugin struct {
	config             *qrmresctrl.ResctrlConfig
	closManager        ClosManager
	rdtManager         rdt.RDTManager
	capabilityProvider rdt.CATCapabilityProvider
	active             bool
}

func NewCATPlugin(conf *config.Configuration) bulkheadapi.Plugin {
	var resctrlConfig *qrmresctrl.ResctrlConfig
	if conf != nil && conf.QRMPluginsConfiguration != nil {
		resctrlConfig = conf.QRMPluginsConfiguration.ResctrlConfig
	}
	if resctrlConfig == nil {
		resctrlConfig = qrmresctrl.NewResctrlConfig()
	} else {
		configCopy := *resctrlConfig
		resctrlConfig = &configCopy
	}
	resctrlConfig.OwnershipCheckpointPath = qrmresctrlmanager.OwnershipCheckpointPath(conf)
	return NewCATPluginWithManager(
		resctrlConfig,
		newConfiguredClosManager(
			resctrlConfig,
			qrmresctrlmanager.NewCPUListManager(resctrlConfig.OwnershipCheckpointPath),
		),
		rdt.NewDefaultManager(),
		rdt.NewCATCapabilityProvider(),
	)
}

func NewCATPluginWithManager(
	config *qrmresctrl.ResctrlConfig,
	closManager ClosManager,
	rdtManager rdt.RDTManager,
	capabilityProvider rdt.CATCapabilityProvider,
) *CATPlugin {
	if config == nil {
		config = qrmresctrl.NewResctrlConfig()
	}
	return &CATPlugin{
		config:             config,
		closManager:        closManager,
		rdtManager:         rdtManager,
		capabilityProvider: capabilityProvider,
	}
}

func (p *CATPlugin) Name() string { return CATPluginName }

func (p *CATPlugin) Enable(in bulkheadapi.HandlerContext) bool {
	return enableCAT(in.DynamicConf)
}

func (p *CATPlugin) CPUSetAdjustmentHandler(ctx context.Context, in bulkheadapi.HandlerContext) error {
	return nil
}

func (p *CATPlugin) CPUSetAdjustmentDisabledHandler(ctx context.Context, in bulkheadapi.HandlerContext) error {
	return nil
}

func (p *CATPlugin) PeriodicalHandler(ctx context.Context, in bulkheadapi.PeriodicalHandlerContext) error {
	if p.closManager == nil || p.rdtManager == nil || p.capabilityProvider == nil {
		return nil
	}
	ways, overrides := catConfig(in.DynamicConf)
	if enableCAT(in.DynamicConf) {
		applied, err := p.reconcile(ctx, ways, overrides)
		if err != nil {
			return err
		}
		p.active = applied
		return nil
	}
	if err := p.rollback(ctx, ways); err != nil {
		return err
	}
	p.active = false
	return nil
}

type catTarget struct {
	clos string
	mask map[int]uint64
}

func (p *CATPlugin) reconcile(ctx context.Context, defaultWays int64, overrides map[string]int64) (bool, error) {
	capabilities, err := p.capabilityProvider.GetCATCapabilities()
	if err != nil {
		if errors.Is(err, rdt.ErrCATUnsupported) {
			return false, nil
		}
		return false, fmt.Errorf("get CAT capabilities: %w", err)
	}
	clos, err := p.closManager.ListCATManagedClos(ctx)
	if err != nil {
		return false, fmt.Errorf("list CAT-managed CLOS: %w", err)
	}
	resolved, err := p.resolveOverrides(overrides)
	if err != nil {
		return false, err
	}
	targets := make([]catTarget, 0, len(clos))
	for _, current := range clos {
		ways := defaultWays
		if configured, ok := resolved[current.ID]; ok {
			ways = configured
		}
		target, err := symmetricTarget(capabilities, ways)
		if err != nil {
			return false, fmt.Errorf("build CAT target for CLOS %q: %w", current.ID, err)
		}
		targets = append(targets, catTarget{clos: current.ID, mask: target})
	}
	for _, target := range targets {
		if err := p.rdtManager.ApplyCAT(target.clos, target.mask); err != nil {
			applyErr := fmt.Errorf("apply CAT for CLOS %q: %w", target.clos, err)
			rollbackTarget, targetErr := catRollbackTarget(capabilities, defaultWays)
			if targetErr != nil {
				return false, apierrors.NewAggregate([]error{applyErr, fmt.Errorf("build CAT rollback target: %w", targetErr)})
			}
			if rollbackErr := p.applyTargetToClos(clos, rollbackTarget); rollbackErr != nil {
				return false, apierrors.NewAggregate([]error{applyErr, rollbackErr})
			}
			return false, applyErr
		}
	}
	return len(targets) > 0, nil
}

func (p *CATPlugin) rollback(ctx context.Context, defaultWays int64) error {
	capabilities, err := p.capabilityProvider.GetCATCapabilities()
	if err != nil {
		if errors.Is(err, rdt.ErrCATUnsupported) {
			return nil
		}
		return fmt.Errorf("get CAT capabilities: %w", err)
	}
	clos, err := p.closManager.ListCATManagedClos(ctx)
	if err != nil {
		return fmt.Errorf("list CAT-managed CLOS for rollback: %w", err)
	}
	target, err := catRollbackTarget(capabilities, defaultWays)
	if err != nil {
		return fmt.Errorf("build CAT rollback target: %w", err)
	}
	return p.applyTargetToClos(clos, target)
}

func catRollbackTarget(capabilities map[int]rdt.CATCapability, defaultWays int64) (map[int]uint64, error) {
	if defaultWays > 0 {
		return symmetricTarget(capabilities, defaultWays)
	}
	if len(capabilities) == 0 {
		return nil, fmt.Errorf("no L3 CAT domains")
	}
	target := make(map[int]uint64, len(capabilities))
	for domain, capability := range capabilities {
		if !isContiguousMask(capability.CBMMask) {
			return nil, fmt.Errorf("domain %d has non-contiguous CAT capability mask %x", domain, capability.CBMMask)
		}
		if bits.OnesCount64(capability.CBMMask) < int(capability.MinCBMBits) {
			return nil, fmt.Errorf("domain %d CAT capability mask %x is smaller than minimum %d",
				domain, capability.CBMMask, capability.MinCBMBits)
		}
		target[domain] = capability.CBMMask
	}
	return target, nil
}

func (p *CATPlugin) applyTargetToClos(clos []qrmresctrlmanager.CPUListClos, target map[int]uint64) error {
	var errs []error
	for _, current := range clos {
		if err := p.rdtManager.ApplyCAT(current.ID, target); err != nil {
			errs = append(errs, fmt.Errorf("restore CAT for CLOS %q: %w", current.ID, err))
		}
	}
	return apierrors.NewAggregate(errs)
}

func (p *CATPlugin) resolveOverrides(overrides map[string]int64) (map[string]int64, error) {
	resolved := make(map[string]int64, len(overrides))
	directClosIDs := make(map[string]struct{}, len(overrides))
	for key := range overrides {
		if _, ok := p.config.CPUSetPoolToSharedSubgroup[key]; ok {
			continue
		}
		directClosIDs[resctrlutil.ResolveCATWayKey(key, p.config)] = struct{}{}
	}
	for key, ways := range overrides {
		closID := resctrlutil.ResolveCATWayKey(key, p.config)
		if _, ok := p.config.CPUSetPoolToSharedSubgroup[key]; ok {
			if _, direct := directClosIDs[closID]; direct {
				continue
			}
		}
		if existing, ok := resolved[closID]; ok && existing != ways {
			return nil, fmt.Errorf("conflicting CAT way configuration for CLOS %q", closID)
		}
		resolved[closID] = ways
	}
	return resolved, nil
}

func symmetricTarget(capabilities map[int]rdt.CATCapability, ways int64) (map[int]uint64, error) {
	if ways <= 0 {
		return nil, fmt.Errorf("CAT ways must be positive, got %d", ways)
	}
	if len(capabilities) == 0 {
		return nil, fmt.Errorf("no L3 CAT domains")
	}
	target := make(map[int]uint64, len(capabilities))
	for domain, capability := range capabilities {
		if !isContiguousMask(capability.CBMMask) {
			return nil, fmt.Errorf("domain %d has non-contiguous CAT capability mask %x", domain, capability.CBMMask)
		}
		if ways < int64(capability.MinCBMBits) || ways > int64(bits.OnesCount64(capability.CBMMask)) {
			return nil, fmt.Errorf("domain %d cannot satisfy %d CAT ways", domain, ways)
		}
		mask, ok := contiguousMask(capability.CBMMask, int(ways))
		if !ok {
			return nil, fmt.Errorf("domain %d has no contiguous mask with %d CAT ways", domain, ways)
		}
		target[domain] = mask
	}
	return target, nil
}

func contiguousMask(available uint64, ways int) (uint64, bool) {
	if ways <= 0 || ways > 64 {
		return 0, false
	}
	for start := 0; start+ways <= 64; start++ {
		mask := ^uint64(0)
		if ways < 64 {
			mask = (uint64(1)<<ways - 1) << start
		}
		if mask&^available == 0 {
			return mask, true
		}
	}
	return 0, false
}

func isContiguousMask(mask uint64) bool {
	if mask == 0 {
		return false
	}
	lowest := mask & -mask
	return (mask+lowest)&mask == 0
}

func enableCAT(conf *dynamicconfig.Configuration) bool {
	if conf == nil || conf.AdminQoSConfiguration == nil || conf.AdminQoSConfiguration.CPUPluginConfiguration == nil {
		return false
	}
	if conf.AdminQoSConfiguration.QRMPluginConfiguration != nil &&
		conf.AdminQoSConfiguration.QRMPluginConfiguration.RDTConfig.DisableRDT {
		return false
	}
	return conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.EnableCAT
}

func catConfig(conf *dynamicconfig.Configuration) (int64, map[string]int64) {
	if conf == nil || conf.AdminQoSConfiguration == nil || conf.AdminQoSConfiguration.CPUPluginConfiguration == nil {
		return 0, nil
	}
	cat := conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig
	return cat.DefaultCATWays, cat.ClosCATWays
}
