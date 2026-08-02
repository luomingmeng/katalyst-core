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

// Package systemservice migrates non-critical systemd services, matching
// daemonset-pod PIDs, and (optionally) certain movable kernel threads out
// of the latency-critical cgroup and into a dedicated "system" cpuset. The
// intent is to keep the latency-critical partition free of surprise CPU
// contention while leaving safety-critical services (kworker, migration,
// ksoftirqd, audit, systemd itself) untouched.
//
// Migration strategy:
//   - Everything goes through a single unified path: userspace PID lookup from
//     the cgroup ROOT's cgroup.procs, kthread-only TID supplementation from
//     ROOT/tasks, per-PID classification via /proc/<pid>/stat
//     (see procfscommon.ProcInfo.IsKThread which reads PF_KTHREAD), and an
//     identity-bound cgroup attach into the target "system" cgroup. The attach
//     changes cgroup membership only; it does not set or otherwise guarantee a
//     PID's scheduler affinity.
//   - Kernel threads (info.IsKThread == true) are migrated only when their
//     comm contains one of the whitelisted substrings
//     (BulkheadSystemKThreadCommSubstrs). This is a positive-list to guard
//     against touching scheduler-critical kthreads (per-CPU migration/N,
//     ksoftirqd/N, kworker/N — none of which are safely movable).
//   - Userspace daemons (info.IsKThread == false) are migrated UNLESS their
//     comm appears in BulkheadSystemdCommBlacklist. Latency-critical daemons
//     (systemd, kubelet, containerd, ...) should be listed there.
//
// When the plugin's dynamic switch transitions from enabled to disabled
// (or the first PeriodicalHandler tick after restart observes disabled),
// a one-shot inverse migration reads every PID/TID currently listed in
// targetRel/cgroup.procs or targetRel/tasks and reattaches it to the cpuset root. It does not
// recurse into child cgroups or filter PIDs by managed status. Subsequent ticks
// while disabled are no-ops.
//
// Every migration is logged at V(2) so operators can audit.
package systemservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	apierrors "k8s.io/apimachinery/pkg/util/errors"

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	cgroupclient "github.com/kubewharf/katalyst-core/pkg/util/cgroup/client"
	cgcommon "github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	utilfs "github.com/kubewharf/katalyst-core/pkg/util/fs"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	procfscommon "github.com/kubewharf/katalyst-core/pkg/util/procfs/common"
)

const SystemServicePluginName = "system_service"

const defaultProcfsPath = "/proc"

const slowAttachThreshold = 200 * time.Millisecond

func migrateSweepLogLevel(elapsed time.Duration) int {
	if elapsed >= slowAttachThreshold {
		return 2
	}
	return 4
}

var _ bulkheadapi.Plugin = (*SystemServicePlugin)(nil)

type SystemServicePlugin struct {
	cfg    bulkheadconfig.BulkheadConfiguration
	fs     utilfs.FS
	proc   procfscommon.ProcReader
	cgroup cgroupclient.CgroupClient
	// pinPID opens a process identity handle. Tests replace it to exercise
	// PID-reuse races without depending on host pidfd support.
	pinPID func(pid int) (io.Closer, error)

	// targetRel is the cgroup-relative path of the system-reclaim target
	// (e.g. "system"). We use the CgroupClient AttachPID interface to migrate
	// processes rather than writing cgroup.procs directly, so only the rel
	// path is needed.
	targetRel string

	// rootCgroupProcsPath is the cgroup ROOT's cgroup.procs file. We only
	// classify processes that live directly in the cgroup root (i.e. tasks
	// kubelet / the container runtime never placed in a managed sub-cgroup,
	// which is exactly the set of host-level systemd services and movable
	// kthreads we may steer). Reading this one file is far cheaper and far
	// more precise than walking every PID in /proc.
	rootCgroupProcsPath string

	// lastPeriodicalEnabled tracks the enable state observed by the previous
	// PeriodicalHandler tick. A nil value means "no prior tick observed"
	// (fresh process); when the first tick observes disabled we must run the
	// one-shot reset to converge state after a restart. Read/written only
	// from PeriodicalHandler, which the bulkhead Manager invokes under
	// Manager.mu — no plugin-local lock is required.
	lastPeriodicalEnabled *bool

	// lastMigratedAppliedViewRevision records the most recent applied-view
	// revision used by a successful enabled migration sweep. It is diagnostic
	// state only: a stable revision remains valid for later sweeps because new
	// processes may enter the root cgroup at any time.
	lastMigratedAppliedViewRevision uint64
}

func NewSystemServicePlugin(conf *config.Configuration) bulkheadapi.Plugin {
	var cfg bulkheadconfig.BulkheadConfiguration
	if conf != nil && conf.CPUQRMPluginConfig != nil && conf.CPUQRMPluginConfig.BulkheadConfiguration != nil {
		cfg = *conf.CPUQRMPluginConfig.BulkheadConfiguration
	}

	// The factory signature cannot return an error, so a missing procfs path
	// falls back to a safe default instead of failing construction. Runtime
	// behavior is still gated by Enable / the dynamic switch.
	procfsPath := cfg.BulkheadSystemServiceProcfsPath
	if procfsPath == "" {
		procfsPath = defaultProcfsPath
	}

	fs := utilfs.NewOSFS()
	return &SystemServicePlugin{
		cfg:    cfg,
		fs:     fs,
		proc:   procfscommon.NewProcReader(fs, procfsPath),
		cgroup: cgroupclient.NewCgroupClient(),
		pinPID: openPIDIdentity,
		// The cpuset cgroup ROOT (mount point on v2, <mount>/cpuset on v1) is
		// the only place we scan for candidate PIDs: processes still sitting in
		// the cpuset root are the host-level services / kthreads not yet claimed
		// by any managed sub-cgroup.
		targetRel:           cfg.BulkheadSystemRelPath,
		rootCgroupProcsPath: cgcommon.GetCgroupRootPath(cgcommon.CgroupSubsysCPUSet) + "/cgroup.procs",
	}
}

func (p *SystemServicePlugin) Name() string { return SystemServicePluginName }

func (p *SystemServicePlugin) Enable(in bulkheadapi.HandlerContext) bool {
	return enableBulkheadSystemService(in.DynamicConf)
}

// CPUSetAdjustmentHandler is intentionally a no-op: all migration runs in
// PeriodicalHandler via cgroup.procs (AttachPID).
func (p *SystemServicePlugin) CPUSetAdjustmentHandler(context.Context, bulkheadapi.HandlerContext) error {
	return nil
}

// CPUSetAdjustmentDisabledHandler is a no-op: when bulkhead is disabled we do
// not proactively revert cgroup placement (there is no safe global undo).
func (p *SystemServicePlugin) CPUSetAdjustmentDisabledHandler(context.Context, bulkheadapi.HandlerContext) error {
	return nil
}

// PeriodicalHandler migrates every eligible root-cgroup PID into the target
// cgroup via identity-bound attach when the plugin's dynamic switch is
// enabled. When the switch transitions from enabled to disabled (or the
// first tick after restart observes disabled), it runs a one-shot reset
// that reads every PID currently listed in targetRel/cgroup.procs and
// reattaches it to the cpuset root. The reset does not recurse into child
// cgroups or filter PIDs by managed status. Subsequent ticks while disabled
// are no-ops.
func (p *SystemServicePlugin) PeriodicalHandler(ctx context.Context, in bulkheadapi.PeriodicalHandlerContext) error {
	enabled := enableBulkheadSystemService(in.DynamicConf)
	if enabled && in.EffectiveEnabled != nil && *in.EffectiveEnabled && !in.AppliedViewValidForPeriodical {
		return nil
	}

	if !enabled {
		// Trigger a reset on enabled → disabled transition, or on the first
		// tick after restart if that first observation is disabled. Steady
		// disabled state is a no-op.
		needsReset := p.lastPeriodicalEnabled == nil || *p.lastPeriodicalEnabled
		if !needsReset {
			return nil
		}
		err := p.resetTargetToRoot(ctx, in)
		if err == nil {
			f := false
			p.lastPeriodicalEnabled = &f
		}
		return err
	}

	err := p.runMigrate(ctx, in)
	// Any observed enabled tick — including early returns from missing target
	// or listing errors — updates the tracker to true so a subsequent real
	// disable transition triggers reset.
	t := true
	p.lastPeriodicalEnabled = &t
	return err
}

// runMigrate performs the enabled-path migration: read root cgroup PIDs,
// classify each via ReadProc, and attach eligible processes to the exact
// device/inode identity published for targetRel. Ineligible / racing PIDs are
// logged at V(4) and skipped without aborting the sweep.
func (p *SystemServicePlugin) runMigrate(ctx context.Context, in bulkheadapi.PeriodicalHandlerContext) error {
	migrateStart := time.Now()
	defer func() {
		migrateElapsed := time.Since(migrateStart)
		general.InfofV(migrateSweepLogLevel(migrateElapsed),
			"system_service: migrate sweep elapsed=%s", migrateElapsed)
	}()

	targetRel, proof, ok, err := p.authorizedMigrationTarget(ctx, in)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	identityAttacher, ok := p.cgroup.(cgroupclient.IdentityBoundPIDAttacher)
	if !ok {
		return fmt.Errorf("identity-bound cgroup attach capability is required for target %q", targetRel)
	}

	candidates, err := p.listRootMigrationCandidates()
	if err != nil {
		emitBulkheadSystemServiceResult(in.Emitter, "migrate", "skipped", "list_root_cgroup_pids")
		return fmt.Errorf("list root cgroup pids: %w", err)
	}

	var attachErrors []error
	for _, candidate := range candidates {
		pid := candidate.pid
		if ctx.Err() != nil {
			emitBulkheadSystemServiceResult(in.Emitter, "migrate", "failed", "context_canceled")
			return fmt.Errorf("context canceled: %w", ctx.Err())
		}
		pin, err := p.pinProcess(pid)
		if err != nil {
			if errors.Is(err, syscall.ESRCH) {
				general.InfofV(4, "system_service: migration skipped exited pid=%d err=%v", pid, err)
				continue
			}
			// Linux pidfd_open may reject a non-leader TID with EINVAL.
			// A task-only userspace thread is already covered by its leader
			// from cgroup.procs; all other pin failures remain fail-closed.
			if candidate.taskOnly && errors.Is(err, syscall.EINVAL) {
				general.InfofV(4, "system_service: migration skipped task-only tid=%d rejected by pidfd_open err=%v", pid, err)
				continue
			}
			attachErrors = append(attachErrors, fmt.Errorf("pin pid %d before migration: %w", pid, err))
			continue
		}
		info, err := p.proc.ReadProc(pid)
		if err != nil {
			// The pinned identity prevents this numeric PID from being reused.
			// A read failure therefore refers only to the listed process.
			_ = pin.Close()
			continue
		}
		if candidate.taskOnly && !info.IsKThread {
			_ = pin.Close()
			continue
		}
		if !p.shouldMigrate(info) {
			_ = pin.Close()
			continue
		}
		attachStart := time.Now()
		// The identity-bound attach pins and verifies the exact cgroup proved by
		// cpuset_topology before writing cgroup.procs.
		err = identityAttacher.AttachPIDWithIdentity(ctx, targetRel, cgroupclient.CgroupIdentity{
			Device: proof.Device,
			Inode:  proof.Inode,
		}, pid)
		attachElapsed := time.Since(attachStart)
		closeErr := pin.Close()
		if attachElapsed >= slowAttachThreshold {
			general.InfofV(2, "system_service: slow cgroup attach, pid=%d comm=%q kthread=%v elapsed=%s err=%v",
				pid, info.Comm, info.IsKThread, attachElapsed, err)
		}
		if err != nil {
			// The PID list is a point-in-time snapshot. ESRCH means the task
			// exited before attach, so this PID already needs no migration and
			// must not keep the applied-view revision pending forever.
			if errors.Is(err, syscall.ESRCH) {
				general.InfofV(4, "system_service: migration skipped exited pid=%d comm=%q err=%v",
					pid, info.Comm, err)
				continue
			}
			general.InfofV(4, "system_service: cgroup migration failed, pid=%d comm=%q kthread=%v err=%v",
				pid, info.Comm, info.IsKThread, err)
			attachErrors = append(attachErrors, fmt.Errorf("attach pid %d to %q: %w", pid, targetRel, err))
			continue
		}
		if closeErr != nil {
			attachErrors = append(attachErrors, fmt.Errorf("close pid identity for %d after migration: %w", pid, closeErr))
			continue
		}
		general.InfofV(2, "system_service: migrated process, pid=%d comm=%q kthread=%v",
			pid, info.Comm, info.IsKThread)
	}
	if len(attachErrors) != 0 {
		emitBulkheadSystemServiceResult(in.Emitter, "migrate", "failed", "attach_error")
		return apierrors.NewAggregate(attachErrors)
	}
	emitBulkheadSystemServiceResult(in.Emitter, "migrate", "success", "")
	p.lastMigratedAppliedViewRevision = in.AppliedViewRevision
	return nil
}

// authorizedMigrationTarget fail-closes the enabled migration path unless the
// manager supplies a newly-published AppliedView and the configured target
// cgroup's current cpuset matches a non-empty per-rel proof carrying a stable
// device/inode identity. This keeps system_service from resampling desired
// topology or trusting a static rel path that cpuset_topology did not authorize
// in the most recent converged publication.
func (p *SystemServicePlugin) authorizedMigrationTarget(
	ctx context.Context,
	in bulkheadapi.PeriodicalHandlerContext,
) (string, model.CgroupRelProof, bool, error) {
	if !in.AppliedViewValidForPeriodical || in.AppliedView == nil {
		general.InfofV(4, "system_service: migration skipped, missing applied view")
		emitBulkheadSystemServiceResult(in.Emitter, "migrate", "skipped", "missing_applied_view")
		return "", model.CgroupRelProof{}, false, nil
	}
	if in.AppliedViewRevision == 0 {
		general.InfofV(4, "system_service: migration skipped, invalid applied view revision=0")
		emitBulkheadSystemServiceResult(in.Emitter, "migrate", "skipped", "stale_applied_view")
		return "", model.CgroupRelProof{}, false, nil
	}

	targetRel := strings.Trim(p.targetRel, "/")
	if targetRel == "" {
		general.InfofV(4, "system_service: migration skipped, empty target rel")
		emitBulkheadSystemServiceResult(in.Emitter, "migrate", "skipped", "empty_target_rel")
		return "", model.CgroupRelProof{}, false, nil
	}
	// Target cgroup not created yet — bail early, cpuset_topology owns
	// creation. Next tick will retry the same AppliedView revision.
	if _, err := p.cgroup.StatDir(ctx, targetRel); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return "", model.CgroupRelProof{}, false, fmt.Errorf("stat target cgroup %q: %w", targetRel, err)
		}
		general.InfofV(4, "system_service: target cgroup missing, skipping, rel=%q err=%v",
			targetRel, err)
		emitBulkheadSystemServiceResult(in.Emitter, "migrate", "skipped", "target_cgroup_missing")
		return "", model.CgroupRelProof{}, false, nil
	}

	proof, proved := in.AppliedView.RelProofByRel[targetRel]
	if !proved || proof.Device == 0 || proof.Inode == 0 || proof.CPUSet.IsEmpty() {
		general.InfofV(4, "system_service: migration skipped, target rel lacks non-empty identity-bound applied proof, rel=%q", targetRel)
		emitBulkheadSystemServiceResult(in.Emitter, "migrate", "skipped", "missing_target_rel_proof")
		return "", model.CgroupRelProof{}, false, nil
	}
	targetCPUSet, err := p.readTargetCPUSet(ctx, targetRel)
	if err != nil {
		return "", model.CgroupRelProof{}, false, err
	}
	if targetCPUSet.IsEmpty() || !targetCPUSet.Equals(proof.CPUSet) {
		general.InfofV(4, "system_service: migration skipped, target rel differs from applied proof, rel=%q target=%s applied=%s",
			targetRel, targetCPUSet.String(), proof.CPUSet.String())
		emitBulkheadSystemServiceResult(in.Emitter, "migrate", "skipped", "target_not_in_applied_view")
		return "", model.CgroupRelProof{}, false, nil
	}
	return targetRel, proof, true, nil
}

func (p *SystemServicePlugin) readTargetCPUSet(ctx context.Context, targetRel string) (machine.CPUSet, error) {
	raw, err := p.cgroup.ReadCgroupFile(ctx, targetRel, "cpuset.cpus.effective")
	if err == nil {
		cpuset, parseErr := machine.Parse(strings.TrimSpace(string(raw)))
		if parseErr != nil {
			return machine.NewCPUSet(), fmt.Errorf("parse target cpuset.cpus.effective %q @ %s: %w", strings.TrimSpace(string(raw)), targetRel, parseErr)
		}
		return cpuset, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return machine.NewCPUSet(), fmt.Errorf("read target cpuset.cpus.effective @ %s: %w", targetRel, err)
	}
	cpuset, err := p.cgroup.ReadCPUSet(ctx, targetRel)
	if err != nil {
		return machine.NewCPUSet(), fmt.Errorf("read target cpuset @ %s: %w", targetRel, err)
	}
	return cpuset, nil
}

// resetTargetToRoot performs the one-shot inverse migration when the plugin's
// dynamic switch transitions from enabled to disabled (or the first tick
// after restart observes disabled). It reads every PID currently in
// targetRel/cgroup.procs and re-attaches it into the cpuset root (rel="")
// via CgroupClient.AttachPID. Any per-PID failure is returned so the disabled
// transition remains pending and a later tick retries the incomplete reset.
// Returning PIDs currently in the target cgroup to root before topology-disabled
// state converges avoids leaving stale system-cgroup ownership behind after the
// feature is disabled.
func (p *SystemServicePlugin) resetTargetToRoot(ctx context.Context, in bulkheadapi.PeriodicalHandlerContext) error {
	if _, err := p.cgroup.StatDir(ctx, p.targetRel); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat target cgroup %q for reset: %w", p.targetRel, err)
		}
		general.InfofV(4, "system_service: reset skipped, target missing, rel=%q err=%v",
			p.targetRel, err)
		emitBulkheadSystemServiceResult(in.Emitter, "reset", "skipped", "target_cgroup_missing")
		return nil
	}

	candidates, err := p.listTargetCgroupCandidates(ctx)
	if err != nil {
		emitBulkheadSystemServiceResult(in.Emitter, "reset", "skipped", "list_target_cgroup_pids")
		return fmt.Errorf("list target cgroup pids: %w", err)
	}

	moved := 0
	var errs []error
	for _, candidate := range candidates {
		pid := candidate.pid
		if ctx.Err() != nil {
			emitBulkheadSystemServiceResult(in.Emitter, "reset", "failed", "context_canceled")
			return fmt.Errorf("context canceled: %w", ctx.Err())
		}
		pin, pinErr := p.pinProcess(pid)
		if pinErr != nil {
			if errors.Is(pinErr, syscall.ESRCH) {
				general.InfofV(4, "system_service: reset skipped exited pid=%d err=%v", pid, pinErr)
				continue
			}
			// A task-only non-leader TID is moved with the userspace leader
			// listed in cgroup.procs, so EINVAL is safe only for this case.
			if candidate.taskOnly && errors.Is(pinErr, syscall.EINVAL) {
				general.InfofV(4, "system_service: reset skipped task-only tid=%d rejected by pidfd_open err=%v", pid, pinErr)
				continue
			}
			errs = append(errs, fmt.Errorf("pin pid %d before reset: %w", pid, pinErr))
			continue
		}
		attachErr := p.cgroup.AttachPID(ctx, "", pid)
		closeErr := pin.Close()
		if attachErr != nil {
			// cgroup.procs is only a point-in-time snapshot. A process may
			// exit after listing and before the attach; ESRCH means it no
			// longer needs to be moved and the reset is already satisfied.
			if errors.Is(attachErr, syscall.ESRCH) {
				general.InfofV(4, "system_service: reset skipped exited pid=%d err=%v", pid, attachErr)
				continue
			}
			general.InfofV(4, "system_service: reset attach failed, pid=%d err=%v", pid, attachErr)
			errs = append(errs, fmt.Errorf("attach pid %d to root: %w", pid, attachErr))
			continue
		}
		if closeErr != nil {
			errs = append(errs, fmt.Errorf("close pid identity for %d after reset: %w", pid, closeErr))
			continue
		}
		general.InfofV(2, "system_service: reset migrated pid=%d back to root cgroup", pid)
		moved++
	}
	if len(errs) != 0 {
		emitBulkheadSystemServiceResult(in.Emitter, "reset", "failed", "attach_error")
		return apierrors.NewAggregate(errs)
	}
	emitBulkheadSystemServiceResult(in.Emitter, "reset", "success", "")
	general.InfofV(4, "system_service: reset complete, scanned=%d moved=%d", len(candidates), moved)
	return nil
}

// shouldMigrate returns true if the process described by info is eligible for
// migration. The decision is defensive by design:
//   - Kernel threads: only migrate when comm contains one of the configured
//     substrings (positive whitelist); skip otherwise so scheduler-critical
//     kthreads are never touched.
//   - Userspace: migrate unless comm exactly matches one of the configured
//     blacklist entries (negative list of latency-critical daemons).
func (p *SystemServicePlugin) shouldMigrate(info procfscommon.ProcInfo) bool {
	if info.IsKThread {
		for _, sub := range p.cfg.BulkheadSystemKThreadCommSubstrs {
			if sub == "" {
				continue
			}
			if strings.Contains(info.Comm, sub) {
				return true
			}
		}
		return false
	}
	for _, b := range p.cfg.BulkheadSystemdCommBlacklist {
		if b != "" && info.Comm == b {
			return false
		}
	}
	return true
}

type migrationCandidate struct {
	pid      int
	taskOnly bool
}

// listRootMigrationCandidates returns process leaders from cgroup.procs plus
// task-only candidates from the v1 tasks file. Classification is deliberately
// deferred until after each PID has been pinned, preventing a task-list PID
// from being reused before ReadProc.
func (p *SystemServicePlugin) listRootMigrationCandidates() ([]migrationCandidate, error) {
	data, err := p.fs.ReadFile(p.rootCgroupProcsPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", p.rootCgroupProcsPath, err)
	}
	leaders := stableUniquePIDs(parsePIDList(data))
	byPID := make(map[int]migrationCandidate, len(leaders))
	for _, pid := range leaders {
		byPID[pid] = migrationCandidate{pid: pid}
	}
	tasks, err := p.fs.ReadFile(rootCgroupTasksPath(p.rootCgroupProcsPath))
	if err == nil {
		for _, tid := range stableUniquePIDs(parsePIDList(tasks)) {
			if _, exists := byPID[tid]; !exists {
				byPID[tid] = migrationCandidate{pid: tid, taskOnly: true}
			}
		}
	}
	pids := make([]int, 0, len(byPID))
	for pid := range byPID {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	out := make([]migrationCandidate, 0, len(pids))
	for _, pid := range pids {
		out = append(out, byPID[pid])
	}
	return out, nil
}

func (p *SystemServicePlugin) pinProcess(pid int) (io.Closer, error) {
	pinPID := p.pinPID
	if pinPID == nil {
		pinPID = openPIDIdentity
	}
	return pinPID(pid)
}

// listTargetCgroupCandidates reads targetRel's cgroup.procs and tasks via
// CgroupClient. Entries found only in tasks are marked so pidfd_open EINVAL
// can be handled without weakening the pin requirement for process leaders.
// tasks is needed for kthreads on cgroup v1; read errors on tasks are ignored
// for compatibility with cgroup clients that expose only cgroup.procs.
func (p *SystemServicePlugin) listTargetCgroupCandidates(ctx context.Context) ([]migrationCandidate, error) {
	data, err := p.cgroup.ReadCgroupFile(ctx, p.targetRel, "cgroup.procs")
	if err != nil {
		return nil, fmt.Errorf("read target cgroup.procs @ %s: %w", p.targetRel, err)
	}
	leaders := stableUniquePIDs(parsePIDList(data))
	byPID := make(map[int]migrationCandidate, len(leaders))
	for _, pid := range leaders {
		byPID[pid] = migrationCandidate{pid: pid}
	}
	if tasks, err := p.cgroup.ReadCgroupFile(ctx, p.targetRel, "tasks"); err == nil {
		for _, tid := range stableUniquePIDs(parsePIDList(tasks)) {
			if _, exists := byPID[tid]; !exists {
				byPID[tid] = migrationCandidate{pid: tid, taskOnly: true}
			}
		}
	}
	pids := make([]int, 0, len(byPID))
	for pid := range byPID {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	out := make([]migrationCandidate, 0, len(pids))
	for _, pid := range pids {
		out = append(out, byPID[pid])
	}
	return out, nil
}

func rootCgroupTasksPath(rootCgroupProcsPath string) string {
	return strings.TrimSuffix(rootCgroupProcsPath, "cgroup.procs") + "tasks"
}

// parsePIDList parses a whitespace-separated cgroup.procs payload into a PID
// slice, skipping malformed / non-positive tokens defensively.
func parsePIDList(data []byte) []int {
	lines := strings.Fields(string(data))
	out := make([]int, 0, len(lines))
	for _, line := range lines {
		pid, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil || pid <= 0 {
			continue
		}
		out = append(out, pid)
	}
	return out
}

func stableUniquePIDs(pids []int) []int {
	if len(pids) == 0 {
		return nil
	}
	sort.Ints(pids)
	out := pids[:0]
	last := 0
	for i, pid := range pids {
		if i > 0 && pid == last {
			continue
		}
		out = append(out, pid)
		last = pid
	}
	return out
}

func enableBulkheadSystemService(conf *dynamicconfig.Configuration) bool {
	if conf == nil || conf.AdminQoSConfiguration == nil || conf.AdminQoSConfiguration.CPUPluginConfiguration == nil {
		return false
	}
	return conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.EnableBulkheadSystemService
}

const metricBulkheadSystemServiceResult = "bulkhead_system_service_result"

func emitBulkheadSystemServiceResult(emitter metrics.MetricEmitter, phase, status, reason string) {
	if emitter == nil {
		return
	}
	_ = emitter.StoreInt64(metricBulkheadSystemServiceResult, 1, metrics.MetricTypeNameCount,
		metrics.MetricTag{Key: "phase", Val: phase},
		metrics.MetricTag{Key: "status", Val: status},
		metrics.MetricTag{Key: "reason", Val: reason},
	)
}
