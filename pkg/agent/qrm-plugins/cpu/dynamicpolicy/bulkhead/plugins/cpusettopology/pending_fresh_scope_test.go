package cpusettopology

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	bulkheadutils "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	metapod "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	cgcommon "github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type strictFreshPendingFetcher struct {
	metapod.PodFetcherStub
	containerErr error
	pods         map[string]*v1.Pod
	podErrs      map[string]error
	freshLookups map[string]int
}

func (f *strictFreshPendingFetcher) GetContainerIDWithContext(
	context.Context, string, string,
) (string, error) {
	return "", f.containerErr
}

func (f *strictFreshPendingFetcher) GetPod(ctx context.Context, podUID string) (*v1.Pod, error) {
	if ctx.Value(metapod.BypassCacheKey) != metapod.BypassCacheTrue {
		return nil, errors.New("fresh pod lookup did not bypass cache")
	}
	if ctx.Value(metapod.StrictBypassCacheKey) != metapod.BypassCacheTrue {
		return nil, errors.New("fresh pod lookup was not strict")
	}
	f.freshLookups[podUID]++
	if err := f.podErrs[podUID]; err != nil {
		return nil, err
	}
	if pod := f.pods[podUID]; pod != nil {
		return pod.DeepCopy(), nil
	}
	return nil, metapod.NewPodNotFoundError(podUID)
}

func pendingScopeTestView(entries map[string]machine.CPUSet) *model.DesiredView {
	byPod := make(map[string]map[string]machine.CPUSet, len(entries))
	for podUID, cpus := range entries {
		byPod[podUID] = map[string]machine.CPUSet{"main": cpus}
	}
	return &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		ContainerCPUSetByPod: byPod,
	}}
}

func pendingScopeTestContext(
	fetcher metapod.PodFetcher,
	view *model.DesiredView,
) bulkheadapi.HandlerContext {
	return bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{
			MetaServer: &metaserver.MetaServer{
				MetaAgent: &agent.MetaAgent{PodFetcher: fetcher},
			},
			Mode: cpusetutil.CPUSetAdjustmentModeAdmission,
		},
		DesiredView: view,
	}
}

func podScopeCandidates(podUID string) []string {
	return []string{
		"kubepods/pod" + podUID,
		"kubepods/besteffort/pod" + podUID,
		"kubepods/burstable/pod" + podUID,
	}
}

func absentCandidateErrors(podUID string) map[string]error {
	out := make(map[string]error, 3)
	for _, rel := range podScopeCandidates(podUID) {
		out[rel] = os.ErrNotExist
	}
	return out
}

func TestPendingContainerErrorsUseStrictFreshPodAndFreshQoS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "pod not found", err: metapod.NewPodNotFoundError("cached")},
		{name: "container not found", err: metapod.ErrContainerNotFound},
		{name: "container not running", err: bulkheadutils.ErrContainerNotRunning},
		{name: "identity changed", err: fmt.Errorf("%w: old=old-id current=new-id", bulkheadutils.ErrContainerIdentityChanged)},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			podUID := "strict-fresh-" + strings.ReplaceAll(tt.name, " ", "-")
			fetcher := &strictFreshPendingFetcher{
				containerErr: tt.err,
				pods: map[string]*v1.Pod{
					podUID: {
						ObjectMeta: metav1.ObjectMeta{UID: types.UID(podUID)},
					},
				},
				podErrs:      map[string]error{},
				freshLookups: map[string]int{},
			}
			p := &CPUSetTopologyPlugin{
				cfg:    bulkheadConfigWithPrimary("kubepods"),
				cgroup: &fakeCgroupClient{},
			}

			res, err := p.buildExpectedCPUSetByRel(context.Background(), pendingScopeTestContext(
				fetcher,
				pendingScopeTestView(map[string]machine.CPUSet{podUID: machine.NewCPUSet(0, 1)}),
			))
			if err != nil {
				t.Fatalf("pending error for live pod must remain pending: %v", err)
			}
			if fetcher.freshLookups[podUID] != 1 {
				t.Fatalf("strict fresh lookups = %d, want 1", fetcher.freshLookups[podUID])
			}
			if len(res.PendingByPod) != 1 {
				t.Fatalf("pending entries = %#v, want one", res.PendingByPod)
			}
			if got := res.PendingByPod[0].NativeQOSClass; got != v1.PodQOSBestEffort {
				t.Fatalf("pending qos = %q, want fresh pod qos %q", got, v1.PodQOSBestEffort)
			}
			if got := res.PendingByPod[0].ScopeRel; got != "kubepods/besteffort/pod"+podUID {
				t.Fatalf("pending scope = %q, want fresh best-effort scope", got)
			}
		})
	}
}

func TestLivePendingPodWithNoMaterializedCandidateIsNotStale(t *testing.T) {
	const subprocessEnv = "KATALYST_TEST_LIVE_PENDING_MULTIPLE_CANDIDATES"
	if os.Getenv(subprocessEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLivePendingPodWithNoMaterializedCandidateIsNotStale$")
		cmd.Env = append(os.Environ(), subprocessEnv+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated candidate-root test failed: %v\n%s", err, output)
		}
		return
	}

	const podUID = "live-multiple-candidates"
	cgcommon.InitKubernetesCGroupPath(cgcommon.CgroupTypeSystemd, []string{
		cgcommon.CgroupFsRootPath,
		cgcommon.CgroupFsRootPathBestEffort,
		cgcommon.CgroupFsRootPathBurstable,
	})
	statErrors := make(map[string]error)
	for _, rel := range relativePendingPodScopeCandidates(
		cgcommon.GetPodRelativeCgroupPathCandidatesForQOS(podUID, v1.PodQOSBestEffort)) {
		statErrors[rel] = os.ErrNotExist
	}
	fetcher := &strictFreshPendingFetcher{
		containerErr: metapod.ErrContainerNotFound,
		pods: map[string]*v1.Pod{
			podUID: {
				ObjectMeta: metav1.ObjectMeta{UID: types.UID(podUID)},
			},
		},
		podErrs:      map[string]error{},
		freshLookups: map[string]int{},
	}
	p := &CPUSetTopologyPlugin{
		cgroup: &fakeCgroupClient{
			statErrors: statErrors,
		},
	}

	res, err := p.buildExpectedCPUSetByRel(context.Background(), pendingScopeTestContext(
		fetcher,
		pendingScopeTestView(map[string]machine.CPUSet{podUID: machine.NewCPUSet(0, 1)}),
	))
	if err != nil {
		t.Fatalf("live pending pod must not fail when candidates are not materialized: %v", err)
	}
	if fetcher.freshLookups[podUID] != 1 {
		t.Fatalf("strict fresh lookups = %d, want 1", fetcher.freshLookups[podUID])
	}
	if len(res.PendingByPod) != 1 {
		t.Fatalf("pending entries = %#v, want one live allocation", res.PendingByPod)
	}
	if got := res.PendingCPUSetUnion(); !got.Equals(machine.NewCPUSet(0, 1)) {
		t.Fatalf("pending CPU union = %s, want 0-1", got.String())
	}
}

func TestPendingContainersResolveFreshScopeOncePerPodAndProtectUnion(t *testing.T) {
	t.Parallel()

	const podUID = "pending-container-union"
	fetcher := &strictFreshPendingFetcher{
		containerErr: metapod.ErrContainerNotFound,
		pods: map[string]*v1.Pod{
			podUID: {
				ObjectMeta: metav1.ObjectMeta{UID: types.UID(podUID)},
			},
		},
		podErrs:      map[string]error{},
		freshLookups: map[string]int{},
	}
	p := &CPUSetTopologyPlugin{
		cfg:    bulkheadConfigWithPrimary("kubepods"),
		cgroup: &fakeCgroupClient{},
	}
	view := &model.DesiredView{CPUSetPartitionView: model.CPUSetPartitionView{
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
			podUID: {
				"main":    machine.NewCPUSet(0, 1),
				"sidecar": machine.NewCPUSet(2, 3),
			},
		},
	}}

	res, err := p.buildExpectedCPUSetByRel(
		context.Background(), pendingScopeTestContext(fetcher, view))
	if err != nil {
		t.Fatalf("buildExpectedCPUSetByRel() error = %v", err)
	}
	if fetcher.freshLookups[podUID] != 1 {
		t.Fatalf("strict fresh lookups = %d, want one pod-level lookup", fetcher.freshLookups[podUID])
	}
	if len(res.PendingByPod) != 2 {
		t.Fatalf("pending entries = %#v, want both containers", res.PendingByPod)
	}
	if got := res.PendingCPUSetUnion(); !got.Equals(machine.NewCPUSet(0, 1, 2, 3)) {
		t.Fatalf("pending CPU union = %s, want 0-3", got.String())
	}
}

func TestFreshAbsentPendingPodCandidateCardinality(t *testing.T) {
	t.Parallel()

	const podUID = "fresh-absent"
	tests := []struct {
		name       string
		statErrors map[string]error
		existing   map[string]bool
		wantScope  string
		wantSkip   bool
		wantErr    error
	}{
		{
			name:       "zero candidates skips stale checkpoint entry",
			statErrors: absentCandidateErrors(podUID),
			wantSkip:   true,
		},
		{
			name:       "one candidate retains concrete scope",
			statErrors: absentCandidateErrors(podUID),
			existing: map[string]bool{
				"kubepods/burstable/pod" + podUID: true,
			},
			wantScope: "kubepods/burstable/pod" + podUID,
		},
		{
			name:       "multiple candidates fail closed",
			statErrors: absentCandidateErrors(podUID),
			existing: map[string]bool{
				"kubepods/pod" + podUID:           true,
				"kubepods/burstable/pod" + podUID: true,
			},
			wantErr: errPendingPodScopeAmbiguous,
		},
		{
			name: "non ENOENT is preserved",
			statErrors: func() map[string]error {
				errs := absentCandidateErrors(podUID)
				errs["kubepods/burstable/pod"+podUID] = os.ErrPermission
				return errs
			}(),
			wantErr: os.ErrPermission,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			statErrors := make(map[string]error, len(tt.statErrors))
			for rel, statErr := range tt.statErrors {
				statErrors[rel] = statErr
			}
			for rel := range tt.existing {
				delete(statErrors, rel)
			}
			fetcher := &strictFreshPendingFetcher{
				containerErr: metapod.ErrContainerNotFound,
				pods:         map[string]*v1.Pod{},
				podErrs:      map[string]error{},
				freshLookups: map[string]int{},
			}
			p := &CPUSetTopologyPlugin{
				cfg: bulkheadConfigWithPrimary("kubepods"),
				cgroup: &fakeCgroupClient{
					existing:   tt.existing,
					statErrors: statErrors,
				},
			}

			res, err := p.buildExpectedCPUSetByRel(context.Background(), pendingScopeTestContext(
				fetcher,
				pendingScopeTestView(map[string]machine.CPUSet{podUID: machine.NewCPUSet(2, 3)}),
			))
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("error = %v, want errors.Is(_, %v)", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildExpectedCPUSetByRel() error = %v", err)
			}
			if fetcher.freshLookups[podUID] != 1 {
				t.Fatalf("strict fresh lookups = %d, want 1", fetcher.freshLookups[podUID])
			}
			if tt.wantSkip {
				if len(res.PendingByPod) != 0 {
					t.Fatalf("stale pod entered pending protection: %#v", res.PendingByPod)
				}
				return
			}
			if len(res.PendingByPod) != 1 || res.PendingByPod[0].ScopeRel != tt.wantScope {
				t.Fatalf("pending entries = %#v, want concrete scope %q", res.PendingByPod, tt.wantScope)
			}
		})
	}
}

func TestIdentityChangedMissingPodCandidateCardinality(t *testing.T) {
	t.Parallel()

	const podUID = "identity-changed-missing"
	tests := []struct {
		name      string
		existing  map[string]bool
		wantScope string
		wantSkip  bool
		wantErr   error
	}{
		{
			name:     "zero candidates skips stale checkpoint entry",
			wantSkip: true,
		},
		{
			name: "one candidate retains concrete scope",
			existing: map[string]bool{
				"kubepods/burstable/pod" + podUID: true,
			},
			wantScope: "kubepods/burstable/pod" + podUID,
		},
		{
			name: "multiple candidates fail closed",
			existing: map[string]bool{
				"kubepods/pod" + podUID:           true,
				"kubepods/burstable/pod" + podUID: true,
			},
			wantErr: errPendingPodScopeAmbiguous,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			statErrors := absentCandidateErrors(podUID)
			for rel := range tt.existing {
				delete(statErrors, rel)
			}
			fetcher := &strictFreshPendingFetcher{
				containerErr: fmt.Errorf("%w: old=old-id current=new-id", bulkheadutils.ErrContainerIdentityChanged),
				pods:         map[string]*v1.Pod{},
				podErrs:      map[string]error{},
				freshLookups: map[string]int{},
			}
			p := &CPUSetTopologyPlugin{
				cfg: bulkheadConfigWithPrimary("kubepods"),
				cgroup: &fakeCgroupClient{
					existing:   tt.existing,
					statErrors: statErrors,
				},
			}

			res, err := p.buildExpectedCPUSetByRel(context.Background(), pendingScopeTestContext(
				fetcher,
				pendingScopeTestView(map[string]machine.CPUSet{podUID: machine.NewCPUSet(4, 5)}),
			))
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("error = %v, want errors.Is(_, %v)", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildExpectedCPUSetByRel() error = %v", err)
			}
			if fetcher.freshLookups[podUID] != 1 {
				t.Fatalf("strict fresh lookups = %d, want 1", fetcher.freshLookups[podUID])
			}
			if tt.wantSkip {
				if len(res.PendingByPod) != 0 {
					t.Fatalf("stale pod entered pending protection: %#v", res.PendingByPod)
				}
				return
			}
			if len(res.PendingByPod) != 1 || res.PendingByPod[0].ScopeRel != tt.wantScope {
				t.Fatalf("pending entries = %#v, want concrete scope %q", res.PendingByPod, tt.wantScope)
			}
		})
	}
}

func TestSelectConcretePendingPodScope(t *testing.T) {
	t.Parallel()

	t.Run("rejects unsafe candidates before lookup", func(t *testing.T) {
		tests := []string{
			"",
			".",
			"..",
			"../escape",
			"kubepods/../escape",
			"unmanaged/podunsafe",
			"/kubepods/podunsafe",
		}
		for _, candidate := range tests {
			candidate := candidate
			t.Run(fmt.Sprintf("%q", candidate), func(t *testing.T) {
				cg := &fakeCgroupClient{}
				p := &CPUSetTopologyPlugin{
					cfg:    bulkheadConfigWithPrimary("kubepods"),
					cgroup: cg,
				}

				_, _, err := p.selectConcretePendingPodScope(
					context.Background(), "unsafe", []string{candidate})
				if err == nil {
					t.Fatalf("candidate %q succeeded, want safety error", candidate)
				}
				if len(cg.statCalls) != 0 {
					t.Fatalf("unsafe candidate reached StatDir: %#v", cg.statCalls)
				}
			})
		}
	})

	t.Run("deduplicates normalized candidates before cardinality", func(t *testing.T) {
		const (
			podUID = "duplicate"
			want   = "kubepods/podduplicate"
		)
		cg := &fakeCgroupClient{existing: map[string]bool{want: true}}
		p := &CPUSetTopologyPlugin{
			cfg:    bulkheadConfigWithPrimary("kubepods"),
			cgroup: cg,
		}

		scope, stale, err := p.selectConcretePendingPodScope(context.Background(), podUID, []string{
			want,
			"kubepods//podduplicate",
			"kubepods/podduplicate/",
		})
		if err != nil {
			t.Fatalf("selectConcretePendingPodScope() error = %v", err)
		}
		if stale || scope != want {
			t.Fatalf("scope=%q stale=%v, want scope=%q stale=false", scope, stale, want)
		}
		if len(cg.statCalls) != 1 || cg.statCalls[0] != want {
			t.Fatalf("StatDir calls = %#v, want one canonical lookup for %q", cg.statCalls, want)
		}
	})

	t.Run("preserves EIO from StatDir", func(t *testing.T) {
		const candidate = "kubepods/podio"
		eio := fmt.Errorf("stat candidate: %w", syscall.EIO)
		cg := &fakeCgroupClient{statErrors: map[string]error{candidate: eio}}
		p := &CPUSetTopologyPlugin{
			cfg:    bulkheadConfigWithPrimary("kubepods"),
			cgroup: cg,
		}

		_, _, err := p.selectConcretePendingPodScope(
			context.Background(), "io", []string{candidate})
		if err != eio {
			t.Fatalf("error = %v, want original EIO %v", err, eio)
		}
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("error = %v, want errors.Is(_, EIO)", err)
		}
	})
}

func TestRemovePodIgnoresUnrelatedStalePendingEntry(t *testing.T) {
	t.Parallel()

	const (
		liveUID  = "remove-pod-live"
		staleUID = "remove-pod-unrelated-stale"
	)
	fetcher := &strictFreshPendingFetcher{
		containerErr: metapod.ErrContainerNotFound,
		pods: map[string]*v1.Pod{
			liveUID: {
				ObjectMeta: metav1.ObjectMeta{UID: types.UID(liveUID)},
			},
		},
		podErrs:      map[string]error{},
		freshLookups: map[string]int{},
	}
	statErrors := absentCandidateErrors(staleUID)
	p := &CPUSetTopologyPlugin{
		cfg: bulkheadConfigWithPrimary("kubepods"),
		cgroup: &fakeCgroupClient{
			statErrors: statErrors,
		},
	}

	res, err := p.buildExpectedCPUSetByRel(context.Background(), pendingScopeTestContext(
		fetcher,
		pendingScopeTestView(map[string]machine.CPUSet{
			liveUID:  machine.NewCPUSet(0, 1),
			staleUID: machine.NewCPUSet(2, 3),
		}),
	))
	if err != nil {
		t.Fatalf("RemovePod-shaped round must ignore unrelated stale entry: %v", err)
	}
	if len(res.PendingByPod) != 1 || res.PendingByPod[0].PodUID != liveUID {
		t.Fatalf("pending entries = %#v, want only live pod", res.PendingByPod)
	}
	if fetcher.freshLookups[liveUID] != 1 || fetcher.freshLookups[staleUID] != 1 {
		t.Fatalf("fresh lookups = %#v, want one per pending pod", fetcher.freshLookups)
	}
}

func bulkheadConfigWithPrimary(primary string) bulkheadconfig.BulkheadConfiguration {
	return bulkheadconfig.BulkheadConfiguration{BulkheadPrimaryRelPath: primary}
}
