package resourcepackage

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type mockResourcePackageState struct {
	attributes   map[string]string
	pinnedCPUSet machine.CPUSet
}

func (m *mockResourcePackageState) GetAttributes() map[string]string {
	if m == nil {
		return nil
	}
	return m.attributes
}

func (m *mockResourcePackageState) GetPinnedCPUSet() machine.CPUSet {
	if m == nil {
		return machine.NewCPUSet()
	}
	return m.pinnedCPUSet
}

func TestGetMatchedPinnedCPUSet(t *testing.T) {
	states := map[string]*mockResourcePackageState{
		"pkg1": {
			attributes:   map[string]string{"disable-reclaim": "true"},
			pinnedCPUSet: machine.NewCPUSet(1, 2),
		},
		"pkg2": {
			attributes:   map[string]string{"disable-reclaim": "false"},
			pinnedCPUSet: machine.NewCPUSet(3, 4),
		},
		"pkg3": nil,
	}

	selector, _ := labels.Parse("disable-reclaim=true")

	res := GetMatchedPinnedCPUSet(states, selector)
	if !reflect.DeepEqual(res.ToSliceInt(), []int{1, 2}) {
		t.Errorf("expected [1, 2], got %v", res.ToSliceInt())
	}

	resEmpty := GetMatchedPinnedCPUSet(states, nil)
	if resEmpty.Size() != 0 {
		t.Errorf("expected empty cpuset, got %v", resEmpty.ToSliceInt())
	}
}

func TestGetNUMAMatchedPinnedCPUSet(t *testing.T) {
	numaStates := map[int]map[string]*mockResourcePackageState{
		0: {
			"pkg1": {
				attributes:   map[string]string{"disable-reclaim": "true"},
				pinnedCPUSet: machine.NewCPUSet(1, 2),
			},
		},
		1: {
			"pkg2": {
				attributes:   map[string]string{"disable-reclaim": "true"},
				pinnedCPUSet: machine.NewCPUSet(3, 4),
			},
		},
	}

	selector, _ := labels.Parse("disable-reclaim=true")
	res := GetNUMAMatchedPinnedCPUSet(numaStates, selector)

	if !reflect.DeepEqual(res[0].ToSliceInt(), []int{1, 2}) {
		t.Errorf("expected [1, 2] for NUMA 0, got %v", res[0].ToSliceInt())
	}
	if !reflect.DeepEqual(res[1].ToSliceInt(), []int{3, 4}) {
		t.Errorf("expected [3, 4] for NUMA 1, got %v", res[1].ToSliceInt())
	}
}

func TestGetMatchedPackages(t *testing.T) {
	states := map[string]*mockResourcePackageState{
		"pkg1": {
			attributes: map[string]string{"disable-reclaim": "true"},
		},
		"pkg2": {
			attributes: map[string]string{"disable-reclaim": "false"},
		},
		"pkg3": nil,
	}

	selector, _ := labels.Parse("disable-reclaim=true")
	res := GetMatchedPackages(states, selector)

	expected := sets.NewString("pkg1")
	if !res.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, res)
	}
}
