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

package systemservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	cgroupclient "github.com/kubewharf/katalyst-core/pkg/util/cgroup/client"
	utilfs "github.com/kubewharf/katalyst-core/pkg/util/fs"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	procfscommon "github.com/kubewharf/katalyst-core/pkg/util/procfs/common"
)

// ---------------------------------------------------------------------------
// fake FS: root cgroup.procs reads
// ---------------------------------------------------------------------------

type fakeFS struct {
	reads   map[string]string // path -> file content (e.g. root cgroup.procs)
	readErr error
}

func newFakeFS() *fakeFS {
	return &fakeFS{reads: map[string]string{}}
}

func (f *fakeFS) ReadFile(p string) ([]byte, error) {
	if f.readErr != nil {
		return nil, f.readErr
	}
	if data, ok := f.reads[p]; ok {
		return []byte(data), nil
	}
	return nil, os.ErrNotExist
}

func (f *fakeFS) WriteFile(string, []byte, os.FileMode) error {
	return errors.New("WriteFile must not be called — use CgroupClient.AttachPID instead")
}
func (f *fakeFS) Exists(string) bool                    { return false }
func (f *fakeFS) ReadDir(string) ([]fs.DirEntry, error) { return nil, errors.New("not used") }

var _ utilfs.FS = (*fakeFS)(nil)

// ---------------------------------------------------------------------------
// fake CgroupClient: records AttachPID calls and controls StatDir presence
// ---------------------------------------------------------------------------

type fakeCgroup struct {
	cgroupclient.FakeCgroupClient
	existingDirs       map[string]bool // rel -> whether StatDir succeeds
	attaches           []attachCall
	identityAttaches   []identityAttachCall
	attachErr          error
	attachHook         func()
	identityAttachHook func()

	// cgroupFiles simulates reading files like cgroup.procs under a given
	// rel. Keys: rel -> file basename -> file bytes. The reset path uses
	// this to enumerate PIDs currently in targetRel/cgroup.procs.
	cgroupFiles   map[string]map[string][]byte
	cgroupFileErr error
	cpuSets       map[string]machine.CPUSet
}

type attachCall struct {
	rel string
	pid int
}

type identityAttachCall struct {
	rel      string
	identity cgroupclient.CgroupIdentity
	pid      int
}

func newFakeCgroup() *fakeCgroup {
	return &fakeCgroup{existingDirs: map[string]bool{}}
}

func (f *fakeCgroup) StatDir(_ context.Context, rel string) (time.Time, error) {
	if f.existingDirs[rel] {
		return time.Time{}, nil
	}
	return time.Time{}, os.ErrNotExist
}

func (f *fakeCgroup) AttachPID(_ context.Context, rel string, pid int) error {
	if f.attachHook != nil {
		f.attachHook()
	}
	if f.attachErr != nil {
		return f.attachErr
	}
	f.attaches = append(f.attaches, attachCall{rel: rel, pid: pid})
	return nil
}

func (f *fakeCgroup) AttachPIDWithIdentity(
	_ context.Context,
	rel string,
	identity cgroupclient.CgroupIdentity,
	pid int,
) error {
	if f.identityAttachHook != nil {
		f.identityAttachHook()
	}
	if f.attachErr != nil {
		return f.attachErr
	}
	f.identityAttaches = append(f.identityAttaches, identityAttachCall{rel: rel, identity: identity, pid: pid})
	f.attaches = append(f.attaches, attachCall{rel: rel, pid: pid})
	return nil
}

func (f *fakeCgroup) ReadCgroupFile(_ context.Context, rel, file string) ([]byte, error) {
	if f.cgroupFileErr != nil {
		return nil, f.cgroupFileErr
	}
	if m, ok := f.cgroupFiles[rel]; ok {
		if data, ok := m[file]; ok {
			return data, nil
		}
	}
	return nil, os.ErrNotExist
}

func (f *fakeCgroup) ReadCPUSet(_ context.Context, rel string) (machine.CPUSet, error) {
	if cpus, ok := f.cpuSets[rel]; ok {
		return cpus, nil
	}
	if f.existingDirs[rel] {
		return machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7), nil
	}
	return machine.NewCPUSet(), os.ErrNotExist
}

// rootProcsPath is the cpuset controller root cgroup.procs path the test
// plugin is wired to; tests seed fakeFS.reads[rootProcsPath] with the
// whitespace-separated PID list the plugin should classify.
const (
	rootProcsPath = "/sys/fs/cgroup/cpuset/cgroup.procs"
	rootTasksPath = "/sys/fs/cgroup/cpuset/tasks"
)

func seedRootPIDs(fFS *fakeFS, pids ...int) {
	var b strings.Builder
	for _, pid := range pids {
		b.WriteString(strconv.Itoa(pid))
		b.WriteByte('\n')
	}
	fFS.reads[rootProcsPath] = b.String()
}

func seedRootTasks(fFS *fakeFS, pids ...int) {
	var b strings.Builder
	for _, pid := range pids {
		b.WriteString(strconv.Itoa(pid))
		b.WriteByte('\n')
	}
	fFS.reads[rootTasksPath] = b.String()
}

// seedTargetPIDs writes a synthetic cgroup.procs into the fake CgroupClient
// under the given targetRel. Used by disable-reset tests to represent
// "these PIDs currently live under the system cgroup".
func seedTargetPIDs(fCg *fakeCgroup, targetRel string, pids ...int) {
	if fCg.cgroupFiles == nil {
		fCg.cgroupFiles = map[string]map[string][]byte{}
	}
	if fCg.cgroupFiles[targetRel] == nil {
		fCg.cgroupFiles[targetRel] = map[string][]byte{}
	}
	var b strings.Builder
	for _, pid := range pids {
		b.WriteString(strconv.Itoa(pid))
		b.WriteByte('\n')
	}
	fCg.cgroupFiles[targetRel]["cgroup.procs"] = []byte(b.String())
}

func seedTargetTasks(fCg *fakeCgroup, targetRel string, pids ...int) {
	if fCg.cgroupFiles == nil {
		fCg.cgroupFiles = map[string]map[string][]byte{}
	}
	if fCg.cgroupFiles[targetRel] == nil {
		fCg.cgroupFiles[targetRel] = map[string][]byte{}
	}
	var b strings.Builder
	for _, pid := range pids {
		b.WriteString(strconv.Itoa(pid))
		b.WriteByte('\n')
	}
	fCg.cgroupFiles[targetRel]["tasks"] = []byte(b.String())
}

func seedTargetEffectiveCPUSet(fCg *fakeCgroup, targetRel, cpus string) {
	if fCg.cgroupFiles == nil {
		fCg.cgroupFiles = map[string]map[string][]byte{}
	}
	if fCg.cgroupFiles[targetRel] == nil {
		fCg.cgroupFiles[targetRel] = map[string][]byte{}
	}
	fCg.cgroupFiles[targetRel]["cpuset.cpus.effective"] = []byte(cpus)
}

// ---------------------------------------------------------------------------
// fake ProcReader
// ---------------------------------------------------------------------------

type fakeProc struct {
	procs    map[int]procfscommon.ProcInfo
	listErr  error
	affinity map[int][]int
	readHook func(int)
}

func (f *fakeProc) ListPIDs() ([]int, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]int, 0, len(f.procs))
	for pid := range f.procs {
		out = append(out, pid)
	}
	sort.Ints(out)
	return out, nil
}

func (f *fakeProc) ReadProc(pid int) (procfscommon.ProcInfo, error) {
	if f.readHook != nil {
		f.readHook(pid)
	}
	info, ok := f.procs[pid]
	if !ok {
		return procfscommon.ProcInfo{}, errors.New("no such pid")
	}
	return info, nil
}

func (f *fakeProc) SchedSetaffinity(pid int, cpus []int) error {
	if f.affinity == nil {
		f.affinity = map[int][]int{}
	}
	cp := make([]int, len(cpus))
	copy(cp, cpus)
	f.affinity[pid] = cp
	return nil
}

var _ procfscommon.ProcReader = (*fakeProc)(nil)

type fakePIDPin struct {
	onClose func()
}

func (p *fakePIDPin) Close() error {
	if p.onClose != nil {
		p.onClose()
	}
	return nil
}

// raceyProcReader wraps fakeProc but returns an error for a specific PID on
// ReadProc to simulate a "process exited between ListPIDs and ReadProc" race.
type raceyProcReader struct {
	*fakeProc
	failingPID int
}

func (r *raceyProcReader) ReadProc(pid int) (procfscommon.ProcInfo, error) {
	if pid == r.failingPID {
		return procfscommon.ProcInfo{}, errors.New("race: process exited")
	}
	return r.fakeProc.ReadProc(pid)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// newTestPlugin builds a plugin with fake fs+proc+cgroup and a fixed
// rootCgroupProcsPath so tests do not depend on a real cgroup mount.
func newTestPlugin(targetRel string, fFS *fakeFS, fProc procfscommon.ProcReader,
	fCg cgroupclient.CgroupClient, cfg bulkheadconfig.BulkheadConfiguration,
) *SystemServicePlugin {
	return &SystemServicePlugin{
		cfg:                 cfg,
		fs:                  fFS,
		proc:                fProc,
		cgroup:              fCg,
		targetRel:           targetRel,
		rootCgroupProcsPath: rootProcsPath,
		pinPID: func(int) (io.Closer, error) {
			return &fakePIDPin{}, nil
		},
	}
}

// dynConf returns a dynamic Configuration with the system_service switch set
// to enabled.
func dynConf(enabled bool) *dynamicconfig.Configuration {
	conf := dynamicconfig.NewConfiguration()
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.EnableBulkheadSystemService = enabled
	return conf
}

func TestMigrateSweepLogLevel_ThresholdBoundary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		elapsed time.Duration
		want    int
	}{
		{name: "below threshold", elapsed: slowAttachThreshold - time.Nanosecond, want: 4},
		{name: "at threshold", elapsed: slowAttachThreshold, want: 2},
		{name: "above threshold", elapsed: slowAttachThreshold + time.Nanosecond, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := migrateSweepLogLevel(tt.elapsed); got != tt.want {
				t.Fatalf("migrateSweepLogLevel(%s) = V(%d), want V(%d)", tt.elapsed, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Enable / Name
// ---------------------------------------------------------------------------

func TestEnable(t *testing.T) {
	t.Parallel()
	p := &SystemServicePlugin{}
	if p.Enable(bulkheadapi.HandlerContext{}) {
		t.Fatalf("Enable must be false when DynamicConf is nil")
	}
	in := bulkheadapi.HandlerContext{}
	in.DynamicConf = dynConf(false)
	if p.Enable(in) {
		t.Fatalf("Enable must be false when switch is off")
	}
	in.DynamicConf = dynConf(true)
	if !p.Enable(in) {
		t.Fatalf("Enable must be true when switch is on")
	}
}

func TestName(t *testing.T) {
	t.Parallel()
	p := &SystemServicePlugin{}
	if p.Name() != SystemServicePluginName {
		t.Fatalf("Name() = %q, want %q", p.Name(), SystemServicePluginName)
	}
}

// ---------------------------------------------------------------------------
// shouldMigrate
// ---------------------------------------------------------------------------

func TestShouldMigrate_KThreadWhitelistSubstr(t *testing.T) {
	t.Parallel()
	p := &SystemServicePlugin{cfg: bulkheadconfig.BulkheadConfiguration{
		BulkheadSystemKThreadCommSubstrs: []string{"kswapd", "kcompactd"},
	}}
	cases := []struct {
		info procfscommon.ProcInfo
		want bool
	}{
		{procfscommon.ProcInfo{Comm: "kswapd0", IsKThread: true}, true},
		{procfscommon.ProcInfo{Comm: "kcompactd1", IsKThread: true}, true},
		{procfscommon.ProcInfo{Comm: "kworker/0", IsKThread: true}, false},
		{procfscommon.ProcInfo{Comm: "migration/1", IsKThread: true}, false},
		{procfscommon.ProcInfo{Comm: "ksoftirqd/0", IsKThread: true}, false},
	}
	for _, c := range cases {
		if got := p.shouldMigrate(c.info); got != c.want {
			t.Fatalf("shouldMigrate(%q, kthread=true) = %v, want %v", c.info.Comm, got, c.want)
		}
	}
}

func TestShouldMigrate_UserspaceBlacklistExactMatch(t *testing.T) {
	t.Parallel()
	p := &SystemServicePlugin{cfg: bulkheadconfig.BulkheadConfiguration{
		BulkheadSystemdCommBlacklist: []string{"systemd", "kubelet", "containerd"},
	}}
	cases := []struct {
		info procfscommon.ProcInfo
		want bool
	}{
		// Anything not on the blacklist is a candidate.
		{procfscommon.ProcInfo{Comm: "crond"}, true},
		{procfscommon.ProcInfo{Comm: "rsyslogd"}, true},
		{procfscommon.ProcInfo{Comm: "sshd"}, true},
		// Blacklisted daemons stay put.
		{procfscommon.ProcInfo{Comm: "systemd"}, false},
		{procfscommon.ProcInfo{Comm: "kubelet"}, false},
		// Exact-match only: prefix collisions must NOT protect.
		{procfscommon.ProcInfo{Comm: "kubeletx"}, true},
	}
	for _, c := range cases {
		if got := p.shouldMigrate(c.info); got != c.want {
			t.Fatalf("shouldMigrate(%q) = %v, want %v", c.info.Comm, got, c.want)
		}
	}
}

func TestShouldMigrate_UserspaceEmptyBlacklistAllowsAll(t *testing.T) {
	t.Parallel()
	p := &SystemServicePlugin{cfg: bulkheadconfig.BulkheadConfiguration{}}
	if !p.shouldMigrate(procfscommon.ProcInfo{Comm: "arbitrary"}) {
		t.Fatalf("empty blacklist ⇒ every userspace comm must be a migration candidate")
	}
}

func TestShouldMigrate_EmptyEntriesIgnored(t *testing.T) {
	t.Parallel()
	p := &SystemServicePlugin{cfg: bulkheadconfig.BulkheadConfiguration{
		BulkheadSystemdCommBlacklist:     []string{"", "systemd", ""},
		BulkheadSystemKThreadCommSubstrs: []string{"", "kswapd"},
	}}
	if p.shouldMigrate(procfscommon.ProcInfo{Comm: "systemd"}) {
		t.Fatalf("empty blacklist entries must not disable real matches (userspace)")
	}
	if !p.shouldMigrate(procfscommon.ProcInfo{Comm: "crond"}) {
		t.Fatalf("empty blacklist entries must not block non-blacklisted comm (userspace)")
	}
	if !p.shouldMigrate(procfscommon.ProcInfo{Comm: "kswapd0", IsKThread: true}) {
		t.Fatalf("empty whitelist entries must not disable real matches (kthread)")
	}
	if p.shouldMigrate(procfscommon.ProcInfo{Comm: "kworker/0", IsKThread: true}) {
		t.Fatalf("kthread outside whitelist must not migrate")
	}
}

// ---------------------------------------------------------------------------
// CPUSetAdjustmentHandler / CPUSetAdjustmentDisabledHandler are no-ops
// ---------------------------------------------------------------------------

func TestCPUSetAdjustmentHandler_IsNoOp(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		400: {PID: 400, Comm: "kswapd0", IsKThread: true, PPID: 2},
		100: {PID: 100, Comm: "crond"},
	}}
	seedRootPIDs(fFS, 100, 400)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{
		BulkheadSystemKThreadCommSubstrs: []string{"kswapd"},
	})

	if err := p.CPUSetAdjustmentHandler(context.Background(), bulkheadapi.HandlerContext{}); err != nil {
		t.Fatalf("CPUSetAdjustmentHandler: %v", err)
	}
	if len(fProc.affinity) != 0 {
		t.Fatalf("CPUSetAdjustmentHandler must NOT invoke SchedSetaffinity, got %+v", fProc.affinity)
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("CPUSetAdjustmentHandler must NOT invoke AttachPID, got %+v", fCg.attaches)
	}
}

func TestCPUSetAdjustmentDisabledHandler_NoOp(t *testing.T) {
	t.Parallel()
	fProc := &fakeProc{}
	p := newTestPlugin("system", newFakeFS(), fProc, newFakeCgroup(), bulkheadconfig.BulkheadConfiguration{})
	if err := p.CPUSetAdjustmentDisabledHandler(context.Background(), bulkheadapi.HandlerContext{}); err != nil {
		t.Fatalf("CPUSetAdjustmentDisabledHandler: %v", err)
	}
	if len(fProc.affinity) != 0 {
		t.Fatalf("disabled handler must not touch affinity, got %+v", fProc.affinity)
	}
}

// ---------------------------------------------------------------------------
// PeriodicalHandler — unified migration path (kthread whitelist + userspace non-blacklist)
// ---------------------------------------------------------------------------

func periodCtx(enabled bool) bulkheadapi.PeriodicalHandlerContext {
	ctx := bulkheadapi.PeriodicalHandlerContext{DynamicConf: dynConf(enabled)}
	if enabled {
		ctx.AppliedView = appliedViewWithReclaim(machine.NewCPUSet(0, 1, 2, 3, 4, 5, 6, 7))
		ctx.AppliedViewRevision = 1
		ctx.AppliedViewValidForPeriodical = true
	}
	return ctx
}

func appliedPeriodCtx(enabled bool, revision uint64, reclaim machine.CPUSet) bulkheadapi.PeriodicalHandlerContext {
	ctx := bulkheadapi.PeriodicalHandlerContext{DynamicConf: dynConf(enabled)}
	if enabled {
		ctx.AppliedView = appliedViewWithReclaim(reclaim)
		ctx.AppliedViewRevision = revision
		ctx.AppliedViewValidForPeriodical = true
	}
	return ctx
}

func appliedViewWithReclaim(reclaim machine.CPUSet) *model.AppliedView {
	view := model.NewDesiredView()
	view.ReclaimEffective = reclaim.Clone()
	applied := view.ToAppliedView()
	applied.CPUSetByRel = map[string]machine.CPUSet{"system": reclaim.Clone()}
	applied.RelProofByRel = map[string]model.CgroupRelProof{
		"system": {Device: 7, Inode: 11, CPUSet: reclaim.Clone()},
	}
	return applied
}

func TestPeriodicalHandler_MigrationUsesAppliedProofIdentity(t *testing.T) {
	t.Parallel()

	fFS := newFakeFS()
	seedRootPIDs(fFS, 100)
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
	}}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})

	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("PeriodicalHandler() error = %v", err)
	}
	if len(fCg.identityAttaches) != 1 {
		t.Fatalf("identity-bound attaches = %+v, want one", fCg.identityAttaches)
	}
	got := fCg.identityAttaches[0]
	if got.rel != "system" || got.pid != 100 ||
		got.identity != (cgroupclient.CgroupIdentity{Device: 7, Inode: 11}) {
		t.Fatalf("identity-bound attach = %+v", got)
	}
}

func TestPeriodicalHandler_PinsPIDBeforeClassificationUntilIdentityAttachCompletes(t *testing.T) {
	t.Parallel()

	events := make([]string, 0, 4)
	fFS := newFakeFS()
	seedRootPIDs(fFS, 100)
	fProc := &fakeProc{
		procs:    map[int]procfscommon.ProcInfo{100: {PID: 100, Comm: "crond"}},
		readHook: func(int) { events = append(events, "read") },
	}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	fCg.identityAttachHook = func() { events = append(events, "attach") }
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	p.pinPID = func(pid int) (io.Closer, error) {
		if pid != 100 {
			t.Fatalf("pin pid = %d, want 100", pid)
		}
		events = append(events, "pin")
		return &fakePIDPin{onClose: func() { events = append(events, "close") }}, nil
	}

	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if got, want := strings.Join(events, ","), "pin,read,attach,close"; got != want {
		t.Fatalf("PID identity lifetime events = %q, want %q", got, want)
	}
}

func TestPeriodicalHandler_PinsTaskOnlyKThreadBeforeClassification(t *testing.T) {
	t.Parallel()

	events := make([]string, 0, 4)
	fFS := newFakeFS()
	seedRootPIDs(fFS)
	seedRootTasks(fFS, 400)
	fProc := &fakeProc{
		procs: map[int]procfscommon.ProcInfo{
			400: {PID: 400, Comm: "kswapd0", IsKThread: true, PPID: 2},
		},
		readHook: func(int) { events = append(events, "read") },
	}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	fCg.identityAttachHook = func() { events = append(events, "attach") }
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{
		BulkheadSystemKThreadCommSubstrs: []string{"kswapd"},
	})
	p.pinPID = func(int) (io.Closer, error) {
		events = append(events, "pin")
		return &fakePIDPin{onClose: func() { events = append(events, "close") }}, nil
	}

	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if got, want := strings.Join(events, ","), "pin,read,attach,close"; got != want {
		t.Fatalf("task-only PID identity lifetime events = %q, want %q", got, want)
	}
}

func TestPeriodicalHandler_PIDFDUnavailableFailsClosedBeforeClassification(t *testing.T) {
	t.Parallel()

	read := false
	fFS := newFakeFS()
	seedRootPIDs(fFS, 100)
	fProc := &fakeProc{
		procs:    map[int]procfscommon.ProcInfo{100: {PID: 100, Comm: "crond"}},
		readHook: func(int) { read = true },
	}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	p.pinPID = func(int) (io.Closer, error) {
		return nil, errors.New("pidfd_open unsupported")
	}

	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err == nil {
		t.Fatal("PeriodicalHandler must fail closed when pidfd_open is unavailable")
	}
	if read || len(fCg.identityAttaches) != 0 {
		t.Fatalf("unsupported pidfd must prevent classification and attach: read=%v attaches=%+v", read, fCg.identityAttaches)
	}
}

func TestPeriodicalHandler_PIDFDOpenESRCHSkipsExitedPID(t *testing.T) {
	t.Parallel()

	read := false
	fFS := newFakeFS()
	seedRootPIDs(fFS, 100)
	fProc := &fakeProc{
		procs:    map[int]procfscommon.ProcInfo{100: {PID: 100, Comm: "crond"}},
		readHook: func(int) { read = true },
	}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	p.pinPID = func(int) (io.Closer, error) {
		return nil, syscall.ESRCH
	}

	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("exited PID must be skipped: %v", err)
	}
	if read || len(fCg.identityAttaches) != 0 {
		t.Fatalf("exited PID must not be classified or attached: read=%v attaches=%+v", read, fCg.identityAttaches)
	}
}

func TestPeriodicalHandler_PIDFDOpenEINVALSkipsTaskOnlyUserspaceThread(t *testing.T) {
	t.Parallel()

	readPIDs := make([]int, 0, 1)
	fFS := newFakeFS()
	seedRootPIDs(fFS, 100)
	seedRootTasks(fFS, 100, 101)
	fProc := &fakeProc{
		procs: map[int]procfscommon.ProcInfo{
			100: {PID: 100, Comm: "crond"},
		},
		readHook: func(pid int) { readPIDs = append(readPIDs, pid) },
	}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	p.pinPID = func(pid int) (io.Closer, error) {
		if pid == 101 {
			return nil, syscall.EINVAL
		}
		return &fakePIDPin{}, nil
	}

	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("task-only userspace thread must be covered by its cgroup.procs leader: %v", err)
	}
	if len(readPIDs) != 1 || readPIDs[0] != 100 {
		t.Fatalf("only the pinned leader may be classified, got reads=%v", readPIDs)
	}
	if len(fCg.identityAttaches) != 1 || fCg.identityAttaches[0].pid != 100 {
		t.Fatalf("only the userspace leader should be attached, got %+v", fCg.identityAttaches)
	}
}

func TestPeriodicalHandler_PIDFDOpenEINVALForLeaderFailsClosed(t *testing.T) {
	t.Parallel()

	read := false
	fFS := newFakeFS()
	seedRootPIDs(fFS, 100)
	seedRootTasks(fFS, 100)
	fProc := &fakeProc{
		procs:    map[int]procfscommon.ProcInfo{100: {PID: 100, Comm: "crond"}},
		readHook: func(int) { read = true },
	}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	p.pinPID = func(int) (io.Closer, error) {
		return nil, syscall.EINVAL
	}

	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err == nil {
		t.Fatal("pidfd_open EINVAL for a cgroup.procs leader must fail closed")
	}
	if read || len(fCg.identityAttaches) != 0 {
		t.Fatalf("an unpinned leader must not be classified or attached: read=%v attaches=%+v", read, fCg.identityAttaches)
	}
}

func TestPeriodicalHandler_MigrationFailsClosedWithoutIdentityAttachCapability(t *testing.T) {
	t.Parallel()

	fFS := newFakeFS()
	seedRootPIDs(fFS, 100)
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
	}}
	inner := newFakeCgroup()
	inner.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, &cgroupClientWithoutIdentity{CgroupClient: inner}, bulkheadconfig.BulkheadConfiguration{})

	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err == nil {
		t.Fatalf("PeriodicalHandler() must fail without identity-bound attach capability")
	}
	if len(inner.attaches) != 0 {
		t.Fatalf("migration must not fall back to path-only AttachPID, got %+v", inner.attaches)
	}
}

type cgroupClientWithoutIdentity struct {
	cgroupclient.CgroupClient
}

func TestPeriodicalHandler_DisabledByConfig(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{100: {PID: 100, Comm: "crond"}}}
	seedRootPIDs(fFS, 100)
	// existingDirs intentionally empty: the first tick observes disabled,
	// triggers reset, and reset early-exits via target_cgroup_missing. No
	// AttachPID calls are expected either way.
	fCg := newFakeCgroup()
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("disabled plugin must produce zero AttachPID calls, got %d", len(fCg.attaches))
	}
	if p.lastPeriodicalEnabled == nil || *p.lastPeriodicalEnabled {
		t.Fatalf("tracker must be &false after disabled tick, got %v", p.lastPeriodicalEnabled)
	}
}

func TestPeriodicalHandler_SkipsWhenTargetMissing(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{}
	fCg := newFakeCgroup() // no existingDirs → StatDir fails
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("no AttachPID calls expected when target cgroup missing, got %d", len(fCg.attaches))
	}
}

// PeriodicalHandler must migrate BOTH whitelisted kthreads AND non-blacklisted
// userspace processes through the same AttachPID path.
func TestPeriodicalHandler_MigratesKThreadAndUserspaceViaAttachPID(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},                                 // userspace, not blacklisted → migrate
		101: {PID: 101, Comm: "rsyslogd"},                              // userspace, not blacklisted → migrate
		200: {PID: 200, Comm: "systemd"},                               // userspace, blacklisted → skip
		201: {PID: 201, Comm: "kubelet"},                               // userspace, blacklisted → skip
		400: {PID: 400, Comm: "kswapd0", IsKThread: true, PPID: 2},     // kthread on whitelist → migrate
		401: {PID: 401, Comm: "kcompactd1", IsKThread: true, PPID: 2},  // kthread on whitelist → migrate
		500: {PID: 500, Comm: "kworker/0", IsKThread: true, PPID: 2},   // kthread NOT on whitelist → skip
		501: {PID: 501, Comm: "migration/1", IsKThread: true, PPID: 2}, // kthread NOT on whitelist → skip
	}}
	seedRootPIDs(fFS, 100, 101, 200, 201, 400, 401, 500, 501)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{
		BulkheadSystemdCommBlacklist:     []string{"systemd", "kubelet"},
		BulkheadSystemKThreadCommSubstrs: []string{"kswapd", "kcompactd"},
	})
	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}

	got := map[int]string{}
	for _, a := range fCg.attaches {
		got[a.pid] = a.rel
	}
	want := map[int]string{100: "system", 101: "system", 400: "system", 401: "system"}
	if len(got) != len(want) {
		t.Fatalf("AttachPID call set mismatch, got=%+v want=%+v", got, want)
	}
	for pid, rel := range want {
		if got[pid] != rel {
			t.Fatalf("pid %d attached to %q, want %q", pid, got[pid], rel)
		}
	}
	if len(fProc.affinity) != 0 {
		t.Fatalf("PeriodicalHandler must never invoke SchedSetaffinity, got %+v", fProc.affinity)
	}
}

func TestPeriodicalHandler_EnabledMigrationTreatsESRCHAsSatisfied(t *testing.T) {
	t.Parallel()

	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "movable"},
	}}
	seedRootPIDs(fFS, 100)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	fCg.cpuSets = map[string]machine.CPUSet{}
	fCg.cpuSets["system"] = machine.NewCPUSet(0, 1)
	fCg.attachErr = syscall.ESRCH
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	in := appliedPeriodCtx(true, 9, machine.NewCPUSet(0, 1))

	if err := p.PeriodicalHandler(context.Background(), in); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if p.lastMigratedAppliedViewRevision != 9 {
		t.Fatalf("last migrated revision = %d, want ESRCH-satisfied revision 9", p.lastMigratedAppliedViewRevision)
	}
}

func TestPeriodicalHandler_MigratesKThreadDiscoveredOnlyInRootTasks_BitsUT(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		400: {PID: 400, Comm: "kswapd0", IsKThread: true, PPID: 2},
	}}
	seedRootPIDs(fFS)
	seedRootTasks(fFS, 400)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{
		BulkheadSystemKThreadCommSubstrs: []string{"kswapd"},
	})
	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 1 || fCg.attaches[0] != (attachCall{rel: "system", pid: 400}) {
		t.Fatalf("root tasks kthread attach = %+v, want system/400", fCg.attaches)
	}
}

func TestPeriodicalHandler_DoesNotMigrateBlacklistedUserspaceThreadFromTasks_BitsUT(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		200: {PID: 200, Comm: "kubelet"},
		201: {PID: 201, Comm: "grpc-worker"},
	}}
	seedRootPIDs(fFS, 200)
	seedRootTasks(fFS, 200, 201)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{
		BulkheadSystemdCommBlacklist: []string{"kubelet"},
	})
	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("blacklisted userspace thread from tasks must not be migrated, got %+v", fCg.attaches)
	}
}

func TestPeriodicalHandler_EmptyBlacklistMigratesAllUserspace(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
		101: {PID: 101, Comm: "systemd"},
		400: {PID: 400, Comm: "kworker/0", IsKThread: true, PPID: 2},
	}}
	seedRootPIDs(fFS, 100, 101, 400)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	// No blacklist AND no kthread whitelist. Every userspace PID should
	// migrate; every kthread should be skipped.
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	got := map[int]bool{}
	for _, a := range fCg.attaches {
		got[a.pid] = true
	}
	if !got[100] || !got[101] {
		t.Fatalf("empty blacklist: every userspace PID must migrate, got=%+v", got)
	}
	if got[400] {
		t.Fatalf("empty kthread whitelist: kthread must NOT migrate, got=%+v", got)
	}
}

func TestPeriodicalHandler_ToleratesReadProcError(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	base := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
		200: {PID: 200, Comm: "crond"},
	}}
	seedRootPIDs(fFS, 100, 200)
	wrapped := &raceyProcReader{fakeProc: base, failingPID: 200}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, wrapped, fCg, bulkheadconfig.BulkheadConfiguration{})
	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 1 || fCg.attaches[0].pid != 100 {
		t.Fatalf("PeriodicalHandler must skip failing ReadProc; got attaches=%+v", fCg.attaches)
	}
}

func TestSystemServiceConsumesAppliedReclaimUnionOrSafeSubset(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
	}}
	seedRootPIDs(fFS, 100)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetEffectiveCPUSet(fCg, "system", "2-3")
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})

	in := appliedPeriodCtx(true, 7, machine.NewCPUSet(2, 3, 4))
	in.AppliedView.CPUSetByRel["system"] = machine.NewCPUSet(2, 3)
	in.AppliedView.RelProofByRel["system"] = model.CgroupRelProof{
		Device: 7, Inode: 11, CPUSet: machine.NewCPUSet(2, 3),
	}
	if err := p.PeriodicalHandler(context.Background(), in); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 1 || fCg.attaches[0].pid != 100 || fCg.attaches[0].rel != "system" {
		t.Fatalf("target cpuset safe subset of AppliedView reclaim must authorize migration, got %+v", fCg.attaches)
	}
	if p.lastMigratedAppliedViewRevision != 7 {
		t.Fatalf("last migrated revision = %d, want 7", p.lastMigratedAppliedViewRevision)
	}
}

func TestPeriodicalSystemServiceDoesNotResampleDesiredPartition(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
	}}
	seedRootPIDs(fFS, 100)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetEffectiveCPUSet(fCg, "system", "0-1")
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{
		BulkheadReclaimRelPaths: []string{"reclaimed"},
	})

	in := appliedPeriodCtx(true, 8, machine.NewCPUSet(2, 3))
	if err := p.PeriodicalHandler(context.Background(), in); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("target outside AppliedView reclaim must not migrate even if static config exists, got %+v", fCg.attaches)
	}
	if p.lastMigratedAppliedViewRevision != 0 {
		t.Fatalf("unauthorized target must not consume revision, got %d", p.lastMigratedAppliedViewRevision)
	}
}

func TestPeriodicalSystemServiceRequiresPerRelAppliedProof(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
	}}
	seedRootPIDs(fFS, 100)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetEffectiveCPUSet(fCg, "system", "0-1")
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})

	in := appliedPeriodCtx(true, 8, machine.NewCPUSet(0, 1, 2, 3))
	in.AppliedView.CPUSetByRel["system"] = machine.NewCPUSet(2, 3)
	if err := p.PeriodicalHandler(context.Background(), in); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("aggregate reclaim membership must not replace per-rel proof, got %+v", fCg.attaches)
	}
}

func TestPeriodicalSystemServiceSkipsWhenAppliedViewMissing(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
	}}
	seedRootPIDs(fFS, 100)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})

	if err := p.PeriodicalHandler(context.Background(), bulkheadapi.PeriodicalHandlerContext{
		DynamicConf: dynConf(true),
	}); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("missing AppliedView must short-circuit migration, got %+v", fCg.attaches)
	}
}

func TestPeriodicalSystemServiceSkipsOldAppliedViewWhenInvalidForPeriodical_BitsUT(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
	}}
	seedRootPIDs(fFS, 100)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetEffectiveCPUSet(fCg, "system", "2-3")
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})

	in := bulkheadapi.PeriodicalHandlerContext{
		DynamicConf:         dynConf(true),
		AppliedView:         appliedViewWithReclaim(machine.NewCPUSet(2, 3)),
		AppliedViewRevision: 9,
	}
	if err := p.PeriodicalHandler(context.Background(), in); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("old AppliedView invalid for current periodical round must not authorize AttachPID, got %+v", fCg.attaches)
	}
	if p.lastMigratedAppliedViewRevision != 0 {
		t.Fatalf("invalid AppliedView must not consume revision, got %d", p.lastMigratedAppliedViewRevision)
	}
}

func TestPeriodicalSystemServiceContinuesScanningSameAppliedViewRevision(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
		101: {PID: 101, Comm: "rsyslogd"},
		102: {PID: 102, Comm: "new-daemon"},
	}}
	seedRootPIDs(fFS, 100, 101)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetEffectiveCPUSet(fCg, "system", "2-3")
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})

	in := appliedPeriodCtx(true, 9, machine.NewCPUSet(2, 3))
	if err := p.PeriodicalHandler(context.Background(), in); err != nil {
		t.Fatalf("first PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 2 {
		t.Fatalf("first fresh applied revision should migrate both userspace pids, got %+v", fCg.attaches)
	}
	fCg.attaches = nil
	seedRootPIDs(fFS, 102)

	if err := p.PeriodicalHandler(context.Background(), in); err != nil {
		t.Fatalf("second PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 1 || fCg.attaches[0] != (attachCall{rel: "system", pid: 102}) {
		t.Fatalf("same applied revision must continue scanning newly arrived PIDs, got %+v", fCg.attaches)
	}
}

func TestPeriodicalHandler_TolerateAttachFailures(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
		101: {PID: 101, Comm: "crond"},
	}}
	seedRootPIDs(fFS, 100, 101)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	fCg.attachErr = errors.New("EBUSY")
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err == nil {
		t.Fatal("PeriodicalHandler must return non-ESRCH per-PID attach errors")
	}
}

func TestPeriodicalHandler_AttachFailureDoesNotConsumeAppliedViewRevision_BitsUT(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
	}}
	seedRootPIDs(fFS, 100)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetEffectiveCPUSet(fCg, "system", "2-3")
	fCg.attachErr = errors.New("EBUSY")
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})

	in := appliedPeriodCtx(true, 11, machine.NewCPUSet(2, 3))
	if err := p.PeriodicalHandler(context.Background(), in); err == nil {
		t.Fatal("first PeriodicalHandler must return non-ESRCH attach errors")
	}
	if p.lastMigratedAppliedViewRevision != 0 {
		t.Fatalf("failed AttachPID must not consume revision, got %d", p.lastMigratedAppliedViewRevision)
	}

	fCg.attachErr = nil
	if err := p.PeriodicalHandler(context.Background(), in); err != nil {
		t.Fatalf("second PeriodicalHandler should retry same revision: %v", err)
	}
	if len(fCg.attaches) != 1 || fCg.attaches[0].pid != 100 || fCg.attaches[0].rel != "system" {
		t.Fatalf("same revision should retry AttachPID after prior failure, got %+v", fCg.attaches)
	}
	if p.lastMigratedAppliedViewRevision != 11 {
		t.Fatalf("successful retry should consume revision 11, got %d", p.lastMigratedAppliedViewRevision)
	}
}

func TestPeriodicalHandler_ContextCancelation(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{100: {PID: 100, Comm: "crond"}}}
	seedRootPIDs(fFS, 100)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	if err := p.PeriodicalHandler(ctx, periodCtx(true)); err == nil {
		t.Fatalf("PeriodicalHandler must report error on canceled ctx")
	}
}

// ---------------------------------------------------------------------------
// PeriodicalHandler — disable reset path
// ---------------------------------------------------------------------------

// helper to fetch tracker as concrete bool for assertions.
func trackerVal(t *testing.T, p *SystemServicePlugin) bool {
	t.Helper()
	if p.lastPeriodicalEnabled == nil {
		t.Fatalf("tracker must not be nil after PeriodicalHandler call")
	}
	return *p.lastPeriodicalEnabled
}

// TestPeriodicalHandler_EnableToDisableTransitionResets exercises the core
// transition: tick1 enabled runs migration; tick2 disabled runs the one-shot
// reset that reattaches every PID currently under targetRel back into the
// cpuset root (rel="").
func TestPeriodicalHandler_EnableToDisableTransitionResets(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
		400: {PID: 400, Comm: "kswapd0", IsKThread: true, PPID: 2},
	}}
	seedRootPIDs(fFS, 100, 400)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{
		BulkheadSystemKThreadCommSubstrs: []string{"kswapd"},
	})

	// tick1: enabled → migrate into "system".
	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("tick1 enabled: %v", err)
	}
	if trackerVal(t, p) != true {
		t.Fatalf("tick1 tracker must be &true, got &false")
	}
	migrateCount := len(fCg.attaches)
	if migrateCount == 0 {
		t.Fatalf("tick1 must produce AttachPID calls, got 0")
	}
	// PIDs 100 and 400 should now be inside targetRel per production semantics;
	// seed the fake cgroup.procs to reflect that.
	seedTargetPIDs(fCg, "system", 100, 400)

	// tick2: disabled → reset every target PID back to rel="".
	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("tick2 disabled: %v", err)
	}
	if trackerVal(t, p) != false {
		t.Fatalf("tick2 tracker must be &false, got &true")
	}
	// Two new AttachPID calls to rel="" must have been recorded.
	resetCalls := fCg.attaches[migrateCount:]
	if len(resetCalls) != 2 {
		t.Fatalf("reset must produce exactly 2 AttachPID calls, got %d (all=%+v)", len(resetCalls), fCg.attaches)
	}
	gotPids := map[int]bool{}
	for _, a := range resetCalls {
		if a.rel != "" {
			t.Fatalf("reset AttachPID must target rel=\"\" (cpuset root), got rel=%q pid=%d", a.rel, a.pid)
		}
		gotPids[a.pid] = true
	}
	if !gotPids[100] || !gotPids[400] {
		t.Fatalf("reset must reattach PIDs 100 and 400 to root, got %+v", gotPids)
	}
}

func TestPeriodicalHandler_ResetPinsPIDUntilRootAttachCompletes(t *testing.T) {
	t.Parallel()

	events := make([]string, 0, 3)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetPIDs(fCg, "system", 100)
	fCg.attachHook = func() { events = append(events, "attach") }
	p := newTestPlugin("system", newFakeFS(), &fakeProc{}, fCg, bulkheadconfig.BulkheadConfiguration{})
	p.pinPID = func(int) (io.Closer, error) {
		events = append(events, "pin")
		return &fakePIDPin{onClose: func() { events = append(events, "close") }}, nil
	}
	enabled := true
	p.lastPeriodicalEnabled = &enabled

	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("PeriodicalHandler reset: %v", err)
	}
	if got, want := strings.Join(events, ","), "pin,attach,close"; got != want {
		t.Fatalf("reset PID identity lifetime events = %q, want %q", got, want)
	}
}

func TestPeriodicalHandler_ResetPIDFDUnavailableFailsClosedAndRemainsPending(t *testing.T) {
	t.Parallel()

	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetPIDs(fCg, "system", 100)
	p := newTestPlugin("system", newFakeFS(), &fakeProc{}, fCg, bulkheadconfig.BulkheadConfiguration{})
	p.pinPID = func(int) (io.Closer, error) {
		return nil, errors.New("pidfd_open unsupported")
	}
	enabled := true
	p.lastPeriodicalEnabled = &enabled

	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err == nil {
		t.Fatal("reset must fail closed when pidfd_open is unavailable")
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("reset must not attach an unpinned numeric PID, got %+v", fCg.attaches)
	}
	if !trackerVal(t, p) {
		t.Fatal("failed reset must remain pending")
	}
}

func TestPeriodicalHandler_ResetPIDFDOpenEINVALSkipsTaskOnlyUserspaceThread(t *testing.T) {
	t.Parallel()

	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetPIDs(fCg, "system", 100)
	seedTargetTasks(fCg, "system", 100, 101)
	p := newTestPlugin("system", newFakeFS(), &fakeProc{}, fCg, bulkheadconfig.BulkheadConfiguration{})
	p.pinPID = func(pid int) (io.Closer, error) {
		if pid == 101 {
			return nil, syscall.EINVAL
		}
		return &fakePIDPin{}, nil
	}
	enabled := true
	p.lastPeriodicalEnabled = &enabled

	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("reset task-only userspace thread must be covered by its cgroup.procs leader: %v", err)
	}
	if len(fCg.attaches) != 1 || fCg.attaches[0] != (attachCall{rel: "", pid: 100}) {
		t.Fatalf("reset must attach only the pinned userspace leader, got %+v", fCg.attaches)
	}
	if trackerVal(t, p) {
		t.Fatal("reset with only a covered task-only thread must complete")
	}
}

func TestPeriodicalHandler_DisabledResetReadsTargetTasks_BitsUT(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		400: {PID: 400, Comm: "kswapd0", IsKThread: true, PPID: 2},
	}}
	seedRootPIDs(fFS)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetPIDs(fCg, "system")
	seedTargetTasks(fCg, "system", 400)
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{
		BulkheadSystemKThreadCommSubstrs: []string{"kswapd"},
	})
	enabled := true
	p.lastPeriodicalEnabled = &enabled

	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("disabled reset: %v", err)
	}
	if len(fCg.attaches) != 1 || fCg.attaches[0] != (attachCall{rel: "", pid: 400}) {
		t.Fatalf("target tasks reset attach = %+v, want root/400", fCg.attaches)
	}
}

// TestPeriodicalHandler_DisabledStableIsNoOp asserts that once the tracker
// has already observed a disabled state, subsequent disabled ticks are pure
// no-ops even if targetRel still has PIDs (which would indicate a partial
// reset from a prior tick).
func TestPeriodicalHandler_DisabledStableIsNoOp(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetPIDs(fCg, "system", 100, 400)
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	f := false
	p.lastPeriodicalEnabled = &f

	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("stable disabled tick must be a no-op, got attaches=%+v", fCg.attaches)
	}
	if trackerVal(t, p) != false {
		t.Fatalf("tracker must stay &false, got &true")
	}
}

// TestPeriodicalHandler_FirstTickDisabledTriggersReset covers the
// "restart while disabled" convergence path: with a nil tracker and disabled
// context, the first tick must still run the reset.
func TestPeriodicalHandler_FirstTickDisabledTriggersReset(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetPIDs(fCg, "system", 100, 400)
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})

	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 2 {
		t.Fatalf("first-tick disabled reset must produce 2 AttachPID calls, got %+v", fCg.attaches)
	}
	for _, a := range fCg.attaches {
		if a.rel != "" {
			t.Fatalf("reset AttachPID must target rel=\"\", got rel=%q pid=%d", a.rel, a.pid)
		}
	}
	if trackerVal(t, p) != false {
		t.Fatalf("tracker must be &false after first-tick disabled, got &true")
	}
}

// TestPeriodicalHandler_DisableThenEnableResumesAndResetsTracker asserts that
// after a disable→reset cycle, a subsequent enable resumes normal migration
// and updates the tracker to &true so the next disable transition can
// trigger reset again.
func TestPeriodicalHandler_DisableThenEnableResumesAndResetsTracker(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{procs: map[int]procfscommon.ProcInfo{
		100: {PID: 100, Comm: "crond"},
	}}
	seedRootPIDs(fFS, 100)
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetPIDs(fCg, "system", 500)
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})

	// tick1 disabled → reset PID 500 to root.
	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("tick1: %v", err)
	}
	if trackerVal(t, p) != false {
		t.Fatalf("tick1 tracker must be &false")
	}
	resetLen := len(fCg.attaches)
	if resetLen != 1 || fCg.attaches[0].rel != "" || fCg.attaches[0].pid != 500 {
		t.Fatalf("tick1 must reset PID 500 to rel=\"\", got %+v", fCg.attaches)
	}

	// tick2 enabled → normal migrate resumes; tracker flips to &true.
	if err := p.PeriodicalHandler(context.Background(), periodCtx(true)); err != nil {
		t.Fatalf("tick2: %v", err)
	}
	if trackerVal(t, p) != true {
		t.Fatalf("tick2 tracker must be &true")
	}
	migrateNew := fCg.attaches[resetLen:]
	if len(migrateNew) != 1 || migrateNew[0].rel != "system" || migrateNew[0].pid != 100 {
		t.Fatalf("tick2 must migrate PID 100 to rel=\"system\", got %+v", migrateNew)
	}

	// tick3 disabled → tracker was &true, so reset must fire again.
	seedTargetPIDs(fCg, "system", 100)
	tick2End := len(fCg.attaches)
	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("tick3: %v", err)
	}
	if trackerVal(t, p) != false {
		t.Fatalf("tick3 tracker must be &false")
	}
	tick3Calls := fCg.attaches[tick2End:]
	if len(tick3Calls) != 1 || tick3Calls[0].rel != "" || tick3Calls[0].pid != 100 {
		t.Fatalf("tick3 must reset PID 100 to rel=\"\", got %+v", tick3Calls)
	}
}

// TestPeriodicalHandler_ResetSkippedWhenTargetMissing asserts reset silently
// bails when targetRel does not exist. Tracker is still advanced to &false.
func TestPeriodicalHandler_ResetSkippedWhenTargetMissing(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{}
	fCg := newFakeCgroup() // no existingDirs → StatDir fails
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	tr := true
	p.lastPeriodicalEnabled = &tr

	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("PeriodicalHandler: %v", err)
	}
	if len(fCg.attaches) != 0 {
		t.Fatalf("reset with missing target must produce zero AttachPID calls, got %+v", fCg.attaches)
	}
	if trackerVal(t, p) != false {
		t.Fatalf("tracker must be &false after skipped reset, got &true")
	}
}

// TestPeriodicalHandler_ResetToleratesAttachPIDErrors asserts per-PID
// AttachPID failures during reset are surfaced so the next disabled tick retries.
func TestPeriodicalHandler_ResetToleratesAttachPIDErrors(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetPIDs(fCg, "system", 100, 200)
	fCg.attachErr = errors.New("EBUSY")
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	tr := true
	p.lastPeriodicalEnabled = &tr

	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err == nil {
		t.Fatal("PeriodicalHandler must surface per-PID reset attach errors")
	}
	if trackerVal(t, p) != true {
		t.Fatalf("tracker must remain pending after reset failures, got &false")
	}
}

func TestPeriodicalHandler_ResetIgnoresExitedPID(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	seedTargetPIDs(fCg, "system", 100)
	fCg.attachErr = fmt.Errorf("write cgroup.procs: %w", syscall.ESRCH)
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	tr := true
	p.lastPeriodicalEnabled = &tr

	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err != nil {
		t.Fatalf("exited PID must be treated as an already-completed reset: %v", err)
	}
	if trackerVal(t, p) {
		t.Fatalf("tracker must become disabled after only stale PID races")
	}
}

// TestPeriodicalHandler_ResetListError asserts that when reading targetRel's
// cgroup.procs fails, PeriodicalHandler surfaces the error and keeps the
// transition pending so a later disabled tick retries.
func TestPeriodicalHandler_ResetListError(t *testing.T) {
	t.Parallel()
	fFS := newFakeFS()
	fProc := &fakeProc{}
	fCg := newFakeCgroup()
	fCg.existingDirs["system"] = true
	fCg.cgroupFileErr = errors.New("boom")
	p := newTestPlugin("system", fFS, fProc, fCg, bulkheadconfig.BulkheadConfiguration{})
	tr := true
	p.lastPeriodicalEnabled = &tr

	if err := p.PeriodicalHandler(context.Background(), periodCtx(false)); err == nil {
		t.Fatalf("PeriodicalHandler must surface listTargetCgroupCandidates error")
	}
	if trackerVal(t, p) != true {
		t.Fatalf("tracker must remain pending after reset listing error, got &false")
	}
}
