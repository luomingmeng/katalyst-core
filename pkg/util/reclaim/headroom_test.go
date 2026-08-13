package reclaim

import (
	"reflect"
	"testing"
)

func TestScaleNUMAHeadroomAppliesPercentageAndUnitScale(t *testing.T) {
	got := ScaleNUMAHeadroom(map[int]float64{
		0: 1.5,
		1: 2.333,
	}, 50, 1000)
	want := map[int]float64{
		0: 750,
		1: 1166.5,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NUMAHeadroomMilliCPU = %#v, want %#v", got, want)
	}
}

func TestScaleNUMAHeadroomAllowsPercentageAboveOneHundred(t *testing.T) {
	if got := ScaleNUMAHeadroom(map[int]float64{0: 1}, 101, 1); !reflect.DeepEqual(got, map[int]float64{0: 1.01}) {
		t.Fatalf("ScaleNUMAHeadroom = %#v, want 1.01", got)
	}
}

func TestGetReclaimedNUMAHeadroomUsesConfiguredConsumerPercentage(t *testing.T) {
	lockGlobalRegistry(t)
	resetRegistry()
	if err := registerConsumer("consumer-a", NewGenericConsumer(newTestConf("/a"), nil)); err != nil {
		t.Fatalf("registerConsumer(%q): %v", "consumer-a", err)
	}

	got := GetReclaimedNUMAHeadroom(
		map[int]float64{0: 1.5, 1: 2},
		newDynamicWithPercentages(map[string]int{"consumer-a": 50}),
		"consumer-a",
	)
	want := map[int]float64{0: 0.75, 1: 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetReclaimedNUMAHeadroom = %#v, want %#v", got, want)
	}
}

func TestGetReclaimedNUMAHeadroomOnlyCountsRegisteredConsumersAndCapsAtOneHundred(t *testing.T) {
	lockGlobalRegistry(t)
	resetRegistry()
	for _, name := range []string{"consumer-a", "consumer-b"} {
		if err := registerConsumer(name, NewGenericConsumer(newTestConf("/"+name), nil)); err != nil {
			t.Fatalf("registerConsumer(%q): %v", name, err)
		}
	}

	got := GetReclaimedNUMAHeadroom(
		map[int]float64{0: 1},
		newDynamicWithPercentages(map[string]int{
			"consumer-a": 60,
			"consumer-b": 60,
			"unknown":    80,
		}),
		"consumer-a",
		"unknown",
		"consumer-b",
	)
	want := map[int]float64{0: 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetReclaimedNUMAHeadroom = %#v, want %#v", got, want)
	}
}

func TestValidateNUMAHeadroomRejectsMismatchedNUMAs(t *testing.T) {
	if err := ValidateNUMAHeadroom(
		map[int]float64{0: 1},
		map[int]float64{1: 1},
	); err == nil {
		t.Fatalf("ValidateNUMAHeadroom succeeded for mismatched NUMAs")
	}
}
