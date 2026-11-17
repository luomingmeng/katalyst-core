package memory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	corepod "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	cgroupmgr "github.com/kubewharf/katalyst-core/pkg/util/cgroup/manager"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type psiTargetFetcher struct {
	corepod.PodFetcherStub
	targets   []metrictypes.PodMetricTarget
	sourceErr error
	resolve   func(metrictypes.PodMetricTarget) (metrictypes.PodMetricTarget, error)
}

func (f *psiTargetFetcher) GetPodList(ctx context.Context, filter func(*v1.Pod) bool) ([]*v1.Pod, error) {
	if f.sourceErr != nil {
		return nil, f.sourceErr
	}
	pods, err := f.PodFetcherStub.GetPodList(ctx, filter)
	if err != nil {
		return nil, err
	}
	for _, target := range f.targets {
		if target.Pod != nil && (filter == nil || filter(target.Pod)) {
			pods = append(pods, target.Pod)
		}
	}
	return pods, nil
}
func (f *psiTargetFetcher) GetPodMetricTargets(context.Context) ([]metrictypes.PodMetricTarget, error) {
	return f.targets, f.sourceErr
}
func (f *psiTargetFetcher) ResolvePodMetricTarget(_ context.Context, p *v1.Pod) (metrictypes.PodMetricTarget, bool, error) {
	for _, target := range f.targets {
		if target.Pod.UID == p.UID {
			if f.resolve != nil {
				x, e := f.resolve(target)
				return x, true, e
			}
			return target, true, nil
		}
	}
	return metrictypes.PodMetricTarget{}, false, nil
}
func psiPod(uid string) *v1.Pod {
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: uid, Namespace: "default", Annotations: map[string]string{apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores}}, Spec: v1.PodSpec{Containers: []v1.Container{{Name: "first"}, {Name: "second"}}}, Status: v1.PodStatus{Phase: v1.PodRunning}}
}

func TestWorkloadPSIUsesWholeSandboxTarget(t *testing.T) {
	mockey.PatchConvey("whole Sandbox targets participate in PSI", t, func() {
		p, err := makeSystemPressureEvictionPlugin(makeConf())
		require.NoError(t, err)
		root := t.TempDir()
		p.workloadPaths = []string{root}
		p.memSomeThreshold = 15
		p.memFullThreshold = 4
		p.memPressureDuration = 0
		p.statCgroup = func(string) (os.FileInfo, error) { return os.Stat(root) }
		f := &psiTargetFetcher{}
		for i := 0; i < 3; i++ {
			pod := psiPod(fmt.Sprintf("target-%d", i))
			f.targets = append(f.targets, metrictypes.PodMetricTarget{Pod: pod, CgroupPath: "/kubepods/burstable/" + pod.Name, MappingRevision: 1})
		}
		p.metaServer.PodFetcher = f
		mockey.Mock(common.GetAbsCgroupPath).To(func(_, suffix string) string { return filepath.Join(root, filepath.Base(suffix)) }).Build()
		mockey.Mock(cgroupmgr.GetMemoryPressureWithAbsolutePath).To(func(string, common.PressureType) (*common.MemoryPressure, error) {
			return &common.MemoryPressure{Avg10: 20}, nil
		}).Build()
		require.NoError(t, p.detectWorkloadMemPSI())
		require.True(t, p.isUnderSystemPressure, "Sandbox PSI must not be lost when its UID has no Kubernetes Pod cgroup")
	})
}

func newPSITest(t *testing.T) (*SystemPressureEvictionPlugin, *psiTargetFetcher, string) {
	t.Helper()
	p, err := makeSystemPressureEvictionPlugin(makeConf())
	require.NoError(t, err)
	root := t.TempDir()
	p.workloadPaths = []string{root}
	p.memSomeThreshold = 15
	p.memFullThreshold = 4
	p.memPressureDuration = 15
	p.syncPeriod = 10 * time.Second
	f := &psiTargetFetcher{}
	p.metaServer.PodFetcher = f
	p.statCgroup = func(string) (os.FileInfo, error) { return os.Stat(root) }
	p.readPSI = func(string, common.PressureType) (*common.MemoryPressure, error) {
		return &common.MemoryPressure{Avg10: 20}, nil
	}
	return p, f, root
}
func psiGroup(root string) []workloadPSITarget {
	result := []workloadPSITarget{}
	for i := 0; i < 3; i++ {
		p := psiPod(fmt.Sprintf("instance-%d", i))
		result = append(result, workloadPSITarget{pod: p, path: filepath.Join(root, p.Name)})
	}
	return result
}
func TestWorkloadPSIContinuousPressure(t *testing.T) {
	for _, breakKind := range []string{"low", "missing", "too-few", "gap", "policy"} {
		t.Run(breakKind, func(t *testing.T) {
			p, _, root := newPSITest(t)
			p.workloadPSIStates = map[string]workloadPSIState{}
			info, err := os.Stat(root)
			require.NoError(t, err)
			start := time.Now()
			group := psiGroup(root)
			action, err := p.evaluateWorkloadPSI(context.Background(), root, info, group, start)
			require.NoError(t, err)
			require.Equal(t, actionNoop, action)
			next := start.Add(10 * time.Second)
			switch breakKind {
			case "low":
				p.readPSI = func(string, common.PressureType) (*common.MemoryPressure, error) {
					return &common.MemoryPressure{}, nil
				}
			case "missing":
				p.readPSI = func(string, common.PressureType) (*common.MemoryPressure, error) {
					return nil, errors.New("read failed")
				}
			case "too-few":
				group = group[:2]
			case "gap":
				next = start.Add(40 * time.Second)
			case "policy":
				p.memPressureDuration = 20
			}
			action, err = p.evaluateWorkloadPSI(context.Background(), root, info, group, next)
			if breakKind == "missing" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, actionNoop, action)
			p.readPSI = func(string, common.PressureType) (*common.MemoryPressure, error) {
				return &common.MemoryPressure{Avg10: 20}, nil
			}
			group = psiGroup(root)
			next = next.Add(10 * time.Second)
			action, err = p.evaluateWorkloadPSI(context.Background(), root, info, group, next)
			require.NoError(t, err)
			require.Equal(t, actionNoop, action)
			next = next.Add(20 * time.Second)
			action, err = p.evaluateWorkloadPSI(context.Background(), root, info, group, next)
			require.NoError(t, err)
			require.Equal(t, actionReclaimedEviction, action)
		})
	}
}
func TestWorkloadPSIRootTimersAreIndependent(t *testing.T) {
	p, _, root := newPSITest(t)
	p.workloadPSIStates = map[string]workloadPSIState{}
	info, err := os.Stat(root)
	require.NoError(t, err)
	now := time.Now()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	_, err = p.evaluateWorkloadPSI(context.Background(), a, info, psiGroup(a), now)
	require.NoError(t, err)
	action, err := p.evaluateWorkloadPSI(context.Background(), b, info, psiGroup(b), now.Add(20*time.Second))
	require.NoError(t, err)
	require.Equal(t, actionNoop, action)
	action, err = p.evaluateWorkloadPSI(context.Background(), a, info, psiGroup(a), now.Add(20*time.Second))
	require.NoError(t, err)
	require.Equal(t, actionReclaimedEviction, action)
}
func TestWorkloadPSINestedAndDuplicateTargets(t *testing.T) {
	p, _, root := newPSITest(t)
	p.memPressureDuration = 0
	p.workloadPSIStates = map[string]workloadPSIState{}
	info, err := os.Stat(root)
	require.NoError(t, err)
	group := psiGroup(root)
	parent := workloadPSITarget{pod: psiPod("parent"), path: root}
	group = append(group, parent)
	// A sibling containing '-' sorts before '/' under raw string ordering.
	// It must not hide a descendant from the ancestor suppression check.
	group = append(group, workloadPSITarget{pod: psiPod("sibling"), path: root + "-sibling"})
	calls := map[string]int{}
	p.readPSI = func(path string, _ common.PressureType) (*common.MemoryPressure, error) {
		calls[path]++
		return &common.MemoryPressure{Avg10: 20}, nil
	}
	action, err := p.evaluateWorkloadPSI(context.Background(), root, info, group, time.Now())
	require.NoError(t, err)
	require.Equal(t, actionEviction, action)
	require.Len(t, calls, 4)
	require.Zero(t, calls[root], "ancestor cgroup must not double count descendants")
	group = append(group, workloadPSITarget{pod: psiPod("duplicate"), path: group[0].path})
	_, err = p.evaluateWorkloadPSI(context.Background(), root, info, group, time.Now())
	require.ErrorContains(t, err, "shared target path")
}
func TestWorkloadPSIMappingAndDirectoryReplacement(t *testing.T) {
	for _, mode := range []string{"mapping", "directory"} {
		t.Run(mode, func(t *testing.T) {
			p, f, root := newPSITest(t)
			pod := psiPod("target")
			target := metrictypes.PodMetricTarget{Pod: pod, CgroupPath: "/target", MappingRevision: 1}
			f.targets = []metrictypes.PodMetricTarget{target}
			other := t.TempDir()
			count := 0
			f.resolve = func(x metrictypes.PodMetricTarget) (metrictypes.PodMetricTarget, error) {
				count++
				if mode == "mapping" && count == 2 {
					x.MappingRevision++
				}
				return x, nil
			}
			p.readPSI = func(string, common.PressureType) (*common.MemoryPressure, error) {
				if mode == "directory" {
					p.statCgroup = func(string) (os.FileInfo, error) { return os.Stat(other) }
				}
				return &common.MemoryPressure{Avg10: 20}, nil
			}
			_, err := p.readTargetPSI(context.Background(), workloadPSITarget{pod: pod, path: root, target: &target})
			require.Error(t, err)
		})
	}
}
func TestWorkloadPSIPathBoundary(t *testing.T) {
	require.True(t, psiPathContains("/root/burstable", "/root/burstable/instance"))
	require.False(t, psiPathContains("/root/burstable", "/root/burstable-other/instance"))
	require.False(t, psiPathContains("/root/burstable", "/root/burstable/../other"))
}

func TestWorkloadPSISharedAndIndependentRoots(t *testing.T) {
	mockey.PatchConvey("instances are grouped by actual cgroup", t, func() {
		p, f, base := newPSITest(t)
		p.memPressureDuration = 0
		rootA, rootB := filepath.Join(base, "a"), filepath.Join(base, "b")
		p.workloadPaths = []string{rootA, rootB}
		ordinary := psiPod("ordinary")
		f.PodList = []*v1.Pod{ordinary}
		for i := 0; i < 2; i++ {
			pod := psiPod(fmt.Sprintf("sandbox-%d", i))
			f.targets = append(f.targets, metrictypes.PodMetricTarget{Pod: pod, CgroupPath: "/a/" + pod.Name, MappingRevision: 1})
		}
		for i := 0; i < 3; i++ {
			pod := psiPod(fmt.Sprintf("independent-%d", i))
			f.targets = append(f.targets, metrictypes.PodMetricTarget{Pod: pod, CgroupPath: "/b/" + pod.Name, MappingRevision: 1})
		}
		mockey.Mock(common.GetPodAbsCgroupPath).To(func(string, string) (string, error) { return filepath.Join(rootA, "ordinary"), nil }).Build()
		mockey.Mock(common.GetAbsCgroupPath).To(func(_, suffix string) string { return filepath.Join(base, suffix) }).Build()
		calls := map[string]int{}
		p.readPSI = func(path string, _ common.PressureType) (*common.MemoryPressure, error) {
			calls[path]++
			return &common.MemoryPressure{Avg10: 20}, nil
		}
		require.NoError(t, p.detectWorkloadMemPSI())
		require.True(t, p.isUnderSystemPressure)
		require.Len(t, p.workloadPSIStates, 2)
		require.Len(t, calls, 8)
		for _, n := range calls {
			require.Equal(t, 1, n)
		}
		// A failing leaf invalidates only its known root, not the independent root.
		p.isUnderSystemPressure = false
		p.systemAction = actionNoop
		p.readPSI = func(path string, _ common.PressureType) (*common.MemoryPressure, error) {
			if strings.HasPrefix(path, rootA+"/") {
				return nil, errors.New("leaf unavailable")
			}
			return &common.MemoryPressure{Avg10: 20}, nil
		}
		require.Error(t, p.detectWorkloadMemPSI())
		require.True(t, p.isUnderSystemPressure)
		require.NotContains(t, p.workloadPSIStates, rootA)
		require.Contains(t, p.workloadPSIStates, rootB)
		// Unavailable inventory cannot be treated as a successful empty source.
		p.isUnderSystemPressure = false
		p.systemAction = actionNoop
		f.sourceErr = errors.New("inventory unavailable")
		require.Error(t, p.detectWorkloadMemPSI())
		require.False(t, p.isUnderSystemPressure)
		require.Empty(t, p.workloadPSIStates)
	})
}

func TestWorkloadPSIKubernetesLowDoesNotGateIndependentRoot(t *testing.T) {
	mockey.PatchConvey("independent root bypasses low Kubernetes PSI", t, func() {
		p, f, base := newPSITest(t)
		p.memPressureDuration = 0
		rootA, rootB := filepath.Join(base, "a"), filepath.Join(base, "b")
		p.workloadPaths = []string{rootA, rootB}
		for i := 0; i < 3; i++ {
			pod := psiPod(fmt.Sprintf("sandbox-%d", i))
			f.targets = append(f.targets, metrictypes.PodMetricTarget{Pod: pod, CgroupPath: "/b/" + pod.Name, MappingRevision: 1})
		}
		mockey.Mock(common.GetAbsCgroupPath).To(func(_, suffix string) string { return filepath.Join(base, suffix) }).Build()
		p.readPSI = func(path string, _ common.PressureType) (*common.MemoryPressure, error) {
			if path == rootA {
				return &common.MemoryPressure{}, nil
			}
			return &common.MemoryPressure{Avg10: 20}, nil
		}
		require.NoError(t, p.detectWorkloadMemPSI())
		require.True(t, p.isUnderSystemPressure)
		require.NotContains(t, p.workloadPSIStates, rootA)
		require.Contains(t, p.workloadPSIStates, rootB)
	})
}

func TestWorkloadPSIPreservesWatermarkActionOnFailure(t *testing.T) {
	p, _, _ := newPSITest(t)
	p.isUnderSystemPressure = true
	p.systemAction = actionReclaimedEviction
	p.readPSI = func(string, common.PressureType) (*common.MemoryPressure, error) {
		return nil, errors.New("PSI unavailable")
	}
	require.Error(t, p.detectWorkloadMemPSI())
	require.True(t, p.isUnderSystemPressure)
	require.Equal(t, actionReclaimedEviction, p.systemAction)
}
