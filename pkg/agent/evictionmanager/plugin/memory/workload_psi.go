package memory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/native"
	v1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

type workloadPSIState struct {
	since, lastSample time.Time
	rootInfo          os.FileInfo
	some, full        int
	duration          int
}
type workloadPSITarget struct {
	pod    *v1.Pod
	path   string
	target *metrictypes.PodMetricTarget
}

func psiPathContains(root, child string) bool {
	rel, err := filepath.Rel(root, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func (s *SystemPressureEvictionPlugin) psiTargets(ctx context.Context) ([]workloadPSITarget, error) {
	fetcher := s.metaServer.PodFetcher
	if fetcher == nil {
		return nil, fmt.Errorf("PSI Pod source is unavailable")
	}
	pods, err := fetcher.GetPodList(ctx, native.PodIsActive)
	if err != nil {
		return nil, err
	}
	var errs []error
	targets := make([]workloadPSITarget, 0, len(pods))
	for _, p := range pods {
		if p == nil {
			continue
		}
		target, handled, resolveErr := s.metaServer.ResolvePodMetricTarget(ctx, p)
		if resolveErr != nil {
			errs = append(errs, fmt.Errorf("PSI target %s mapping: %w", p.UID, resolveErr))
			continue
		}
		if handled {
			if target.Pod == nil || target.Pod.UID != p.UID || target.CgroupPath == "" {
				errs = append(errs, fmt.Errorf("PSI target %s identity is incomplete", p.UID))
				continue
			}
			targets = append(targets, workloadPSITarget{pod: p, path: common.GetAbsCgroupPath(common.CgroupSubsysMemory, target.CgroupPath), target: &target})
			continue
		}
		qos, qosErr := s.qosConf.GetQoSLevelForPod(p)
		if qosErr != nil {
			// Keep the pre-existing tolerance: a malformed annotation is logged,
			// not promoted into a per-cycle heartbeat failure.
			general.Errorf("get qos level failed for pod %+v/%+v, skip PSI, err: %v", p.Namespace, p.Name, qosErr)
			continue
		}
		path, e := common.GetPodAbsCgroupPath(common.CgroupSubsysMemory, string(p.UID))
		if e != nil {
			if s.supportedQosLevels.Has(qos) {
				errs = append(errs, fmt.Errorf("PSI Pod %s path: %w", p.UID, e))
			}
			continue
		}
		targets = append(targets, workloadPSITarget{pod: p, path: path})
	}
	return targets, utilerrors.NewAggregate(errs)
}

// readPSIWithIdentity accepts a sample only when the same cgroup directory served
// it before and after the read: a recycled path must not inherit an old sample.
func (s *SystemPressureEvictionPlugin) readPSIWithIdentity(path string) (*common.MemoryPressure, os.FileInfo, error) {
	before, err := s.statCgroup(path)
	if err != nil {
		return nil, nil, err
	}
	value, err := s.readPSI(path, common.SOME)
	if err != nil {
		return nil, nil, err
	}
	after, err := s.statCgroup(path)
	if err != nil {
		return nil, nil, err
	}
	if !os.SameFile(before, after) {
		return nil, nil, fmt.Errorf("PSI cgroup %s changed during read", path)
	}
	return value, after, nil
}

// readTargetPSI keeps the whole-workload binding checked around a shared sample.
func (s *SystemPressureEvictionPlugin) readTargetPSI(ctx context.Context, t workloadPSITarget) (*common.MemoryPressure, error) {
	checkMapping := func() error {
		if t.target == nil {
			return nil
		}
		current, handled, err := s.metaServer.ResolvePodMetricTarget(ctx, t.pod)
		if err != nil {
			return err
		}
		if !handled || !t.target.SameMapping(current) {
			return fmt.Errorf("PSI target %s mapping changed", t.pod.UID)
		}
		return nil
	}
	if err := checkMapping(); err != nil {
		return nil, err
	}
	value, _, err := s.readPSIWithIdentity(t.path)
	if err != nil {
		return nil, err
	}
	if err := checkMapping(); err != nil {
		return nil, err
	}
	return value, nil
}

func (s *SystemPressureEvictionPlugin) detectWorkloadMemPSI() error {
	roots := s.workloadPaths
	// Drop timers for roots that are no longer configured: a removed root must
	// not resume an old start time when it is added back later.
	states := make(map[string]workloadPSIState, len(roots))
	for _, root := range roots {
		if state, ok := s.workloadPSIStates[root]; ok {
			states[root] = state
		}
	}
	s.workloadPSIStates = states

	high := make(map[string]os.FileInfo, len(roots))
	var errs []error
	for _, root := range roots {
		value, after, e := s.readPSIWithIdentity(root)
		if e != nil {
			delete(s.workloadPSIStates, root)
			errs = append(errs, fmt.Errorf("PSI root %s: %w", root, e))
			continue
		}
		if value.Avg10 < uint64(s.memSomeThreshold) {
			delete(s.workloadPSIStates, root)
			continue
		}
		high[root] = after
	}
	if len(high) == 0 {
		return utilerrors.NewAggregate(errs)
	}
	ctx := context.Background()
	targets, e := s.psiTargets(ctx)
	if e != nil {
		// With an incomplete source, unknown members could belong to any root.
		// Conservatively discard this cycle, without erasing watermark pressure.
		for root := range high {
			delete(s.workloadPSIStates, root)
		}
		errs = append(errs, e)
		return utilerrors.NewAggregate(errs)
	}
	groups := make(map[string][]workloadPSITarget)
	seenUID := make(map[string]bool)
	for _, t := range targets {
		if seenUID[string(t.pod.UID)] {
			for root := range high {
				delete(s.workloadPSIStates, root)
			}
			return fmt.Errorf("duplicate PSI target UID %s", t.pod.UID)
		}
		seenUID[string(t.pod.UID)] = true
		for _, root := range roots {
			if root != t.path && psiPathContains(root, t.path) {
				groups[root] = append(groups[root], t)
				break
			}
		}
	}
	now := time.Now()
	for _, root := range roots {
		info, ok := high[root]
		if !ok {
			continue
		}
		action, e := s.evaluateWorkloadPSI(ctx, root, info, groups[root], now)
		if e != nil {
			errs = append(errs, e)
			continue
		}
		if action > actionNoop {
			s.isUnderSystemPressure = true
			if action > s.systemAction {
				s.systemAction = action
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}

func (s *SystemPressureEvictionPlugin) evaluateWorkloadPSI(ctx context.Context, root string, info os.FileInfo, targets []workloadPSITarget, now time.Time) (int, error) {
	fail := func(err error) (int, error) { delete(s.workloadPSIStates, root); return actionNoop, err }
	// The trailing separator keeps a parent adjacent to all its descendants;
	// plain strings would let "-sibling" sort between them.
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].path+string(filepath.Separator) < targets[j].path+string(filepath.Separator)
	})
	var total, count, pressured uint64
	for i, t := range targets {
		key := t.path + string(filepath.Separator)
		// The earlier entry of an adjacent ancestor/descendant pair is the
		// ancestor: skip it so one Sandbox is counted once, via its leaf cgroup.
		if i+1 < len(targets) {
			next := targets[i+1]
			if t.path == next.path {
				return fail(fmt.Errorf("PSI root %s has shared target path %s", root, t.path))
			}
			if strings.HasPrefix(next.path+string(filepath.Separator), key) {
				continue
			}
		}
		qos, e := s.qosConf.GetQoSLevelForPod(t.pod)
		if e != nil {
			return fail(e)
		}
		if !s.supportedQosLevels.Has(qos) {
			continue
		}
		value, e := s.readTargetPSI(ctx, t)
		if e != nil {
			return fail(fmt.Errorf("PSI target %s: %w", t.pod.UID, e))
		}
		if value.Avg10 > minMemPressure {
			pressured++
		}
		total += value.Avg10
		count++
	}
	// Recheck the root after the instance reads, not just after the root read.
	after, e := s.statCgroup(root)
	if e != nil {
		return fail(e)
	}
	if !os.SameFile(info, after) {
		return fail(fmt.Errorf("PSI root %s changed during sampling", root))
	}
	if pressured < minPods || count == 0 || total/count < uint64(s.memFullThreshold) {
		return fail(nil)
	}
	state := s.workloadPSIStates[root]
	if state.since.IsZero() || state.some != s.memSomeThreshold || state.full != s.memFullThreshold ||
		state.duration != s.memPressureDuration || state.rootInfo == nil || !os.SameFile(state.rootInfo, info) ||
		now.Before(state.lastSample) || now.Sub(state.lastSample) > time.Duration(syncTolerationTurns)*s.syncPeriod {
		state = workloadPSIState{
			since: now, rootInfo: info,
			some: s.memSomeThreshold, full: s.memFullThreshold, duration: s.memPressureDuration,
		}
	}
	state.lastSample = now
	s.workloadPSIStates[root] = state
	elapsed := now.Sub(state.since).Seconds()
	action := actionNoop
	if elapsed >= float64(s.memPressureDuration) {
		action = actionReclaimedEviction
	}
	if elapsed >= 3*float64(s.memPressureDuration) {
		action = actionEviction
	}
	general.Infof("workload PSI root=%s some_avg10=%d pressured=%d samples=%d duration=%.1f action=%d", root, total/count, pressured, count, elapsed, action)
	return action, nil
}
