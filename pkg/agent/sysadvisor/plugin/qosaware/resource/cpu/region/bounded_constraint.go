package region

import (
	"math"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

// Stable, low-cardinality bounded restrict observations. The fixed tag set is:
// region_type, region_name, target_policy, reference_policy, control_knob,
// mode, action. region_name is the only addition versus the pre-existing
// restrict series: a node hosts multiple QoS regions of the same region_type
// (e.g. several share regions across NUMA bindings), and without it those
// series would alias. The name is the controlled per-region UUID dimension
// assigned by the region factory (low-cardinality, not a per-pod or per-PID
// string).
//
// action values:
//   - passthrough:  within bound, no overshoot this side.
//   - observed:     shadow mode, overshoot detected, no clamp applied.
//   - clamped:      enforce mode, overshoot detected, pre-regulation target
//                   clamp applied. Later regulator stages keep their own
//                   progressive semantics; this clamp is the bounded path's own.
//   - invalid-config / ref-unavailable / no-positive-tolerance /
//     conflicting-bounds / target-unavailable: fail-open paths; raw stands.
const (
	restrictActionPassthrough         = "passthrough"
	restrictActionObserved            = "observed"
	restrictActionClamped             = "clamped"
	restrictActionTargetUnavailable   = "target-unavailable"
	restrictActionRefUnavailable      = "ref-unavailable"
	restrictActionNoPositiveTolerance = "no-positive-tolerance"
	restrictActionInvalidConfig       = "invalid-config"
	restrictActionConflictingBounds   = "conflicting-bounds"

	restrictModeShadow  = "shadow"
	restrictModeEnforce = "enforce"

	// restrictUnknownPolicy is the normalized label value for unknown/missing
	// policies/knobs so the metric never carries an arbitrary configured string.
	restrictUnknownPolicy = "unknown"
)

// constraintShape classifies an AQC ControlKnobConstraints entry by which of
// targetPolicy/referencePolicy are set.
type constraintShape int

const (
	// shapeNameOnly has neither policy set; the bounded path skips it (the exact
	// path consumes name-only quota entries independently).
	shapeNameOnly constraintShape = iota
	// shapeBounded has both policies set and is handled by the bounded path.
	shapeBounded
	// shapeInvalid has exactly one policy set; fail-open, emit invalid-config.
	shapeInvalid
)

// classifyConstraintShape returns the shape of a constraint entry.
func classifyConstraintShape(c *configapi.ControlKnobConstraints) constraintShape {
	hasTarget := c.TargetPolicy != ""
	hasRef := c.ReferencePolicy != ""
	switch {
	case hasTarget && hasRef:
		return shapeBounded
	case !hasTarget && !hasRef:
		return shapeNameOnly
	default:
		return shapeInvalid
	}
}

// tolerance computes the one-sided extra around ref: the minimum positive of
// the absolute gap and ratio*ref. on is true iff either gap or ratio is
// positive. Shared by the upper and lower sides.
func tolerance(ref, gap, ratio float64) (extra float64, on bool) {
	on = gap > 0 || ratio > 0
	if !on {
		return 0, false
	}
	extra = math.Inf(1)
	if gap > 0 {
		extra = general.MinFloat64(extra, gap)
	}
	if ratio > 0 {
		extra = general.MinFloat64(extra, ratio*ref)
	}
	return extra, true
}

// evaluateBounds computes upper/lower tolerances around ref independently.
// Either side may be on. A single rule can never invert its own bounds: with
// ref>0 and positive extras, lower = max(0, ref-extra) <= ref < ref+extra = upper,
// so lower>upper is unreachable inside one rule and is not special-cased. Only
// composition of rules anchored on different references can produce a
// maxLower>minUpper conflict, which the aggregator fails open. upperOver/lowerOver
// report whether raw crosses the corresponding side's bound. errAction is
// non-empty on fail-open paths (ref-unavailable, no-positive-tolerance) and
// carries the stable action label; on success it stays empty.
func evaluateBounds(raw, ref, upperGap, upperRatio, lowerGap, lowerRatio float64) (
	upper, lower float64, upperOn, lowerOn, upperOver, lowerOver bool, errAction string,
) {
	if math.IsNaN(ref) || math.IsInf(ref, 0) || ref <= 0 {
		return 0, 0, false, false, false, false, restrictActionRefUnavailable
	}
	upperExtra, upperOn := tolerance(ref, upperGap, upperRatio)
	lowerExtra, lowerOn := tolerance(ref, lowerGap, lowerRatio)
	if !upperOn && !lowerOn {
		return 0, 0, false, false, false, false, restrictActionNoPositiveTolerance
	}
	if upperOn {
		upper = ref + upperExtra
	}
	if lowerOn {
		lower = math.Max(0, ref-lowerExtra)
	}
	upperOver = upperOn && raw > upper
	lowerOver = lowerOn && raw < lower
	return upper, lower, upperOn, lowerOn, upperOver, lowerOver, ""
}

// compKey identifies one (target, ref, knob) constraint. Used both for
// de-duplicating explicit entries and as the composite key on the result.
type compKey struct {
	target types.CPUProvisionPolicyName
	ref    types.CPUProvisionPolicyName
	knob   configapi.ControlKnobName
}

// boundedConstraintResult is the slim outcome of evaluating one bounded
// constraint. It carries the composite key, labels needed for the metric, the
// bounds and overshoot flags the aggregator consumes. Mode/action are derived
// at emit time from enforce + overshoot; errAction is non-empty on fail-open
// paths.
type boundedConstraintResult struct {
	key      compKey
	enforce  bool
	targetOK bool
	refOK    bool
	rawOK    bool
	raw      float64

	upperOn   bool
	lowerOn   bool
	upper     float64
	lower     float64
	upperOver bool
	lowerOver bool

	errAction string // non-empty => fail-open; emit one upper gauge with raw.
}

// knobKey identifies a per-(policy,knob) clamp aggregate.
type knobKey struct {
	policy types.CPUProvisionPolicyName
	knob   configapi.ControlKnobName
}

// aggregate composes enforce clamps across rules on the same (target,knob):
// minUpper across upper rules, maxLower across lower rules.
type aggregate struct {
	minUpper float64
	maxLower float64
	hasUpper bool
	hasLower bool
}

func (a *aggregate) addUpper(v float64) {
	if !a.hasUpper || v < a.minUpper {
		a.minUpper = v
		a.hasUpper = true
	}
}

func (a *aggregate) addLower(v float64) {
	if !a.hasLower || v > a.maxLower {
		a.maxLower = v
		a.hasLower = true
	}
}

func (a *aggregate) conflict() bool {
	return a.hasUpper && a.hasLower && a.maxLower > a.minUpper
}

// applyBoundedControlKnobConstraints consumes the AQC ControlKnobConstraints
// slice directly. It iterates constraints once in reverse so that, for an
// explicit (target,ref,knob) duplicate, the last array entry wins (first
// encountered in reverse). Invalid half-explicit entries are not deduped and
// each emits an observation. The bounded path requires both targetPolicy and
// referencePolicy to be explicit; it reads ExactRestrictRefPolicy never, and
// ctrlKnobsNeedPolicyExactRestrict only for ownership rejection. All evaluation
// reads the original input map; clamps are applied once after the scan.
//
// Composition: every successfully-evaluated enforce result contributes each of
// its enabled bounds (upper and/or lower) to the per-(target,knob) aggregate,
// regardless of whether raw currently overshoots that side. Overshoot only
// selects the per-rule metric action label; it never gates aggregation. This is
// what lets two rules on different references surface a maxLower>minUpper
// conflict (which then fails open) instead of silently clamping one side.
func (r *QoSRegionBase) applyBoundedControlKnobConstraints(
	controlKnobs map[types.CPUProvisionPolicyName]types.ControlKnob,
) map[types.CPUProvisionPolicyName]types.ControlKnob {
	constraints := r.DynamicConfiguration.ControlKnobConstraints
	if len(constraints) == 0 {
		return controlKnobs
	}

	seen := make(map[compKey]struct{})
	clamp := make(map[knobKey]*aggregate)

	for i := len(constraints) - 1; i >= 0; i-- {
		c := &constraints[i]
		shape := classifyConstraintShape(c)
		if shape == shapeNameOnly {
			continue
		}
		key := compKey{
			target: types.CPUProvisionPolicyName(c.TargetPolicy),
			ref:    types.CPUProvisionPolicyName(c.ReferencePolicy),
			knob:   c.Name,
		}
		if shape == shapeBounded {
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
		}
		res := r.evaluateBoundedConstraint(controlKnobs, key, c)
		r.emitBoundedResult(res)
		if !res.enforce || res.errAction != "" {
			continue
		}
		k := knobKey{policy: key.target, knob: key.knob}
		agg, ok := clamp[k]
		if !ok {
			agg = &aggregate{}
			clamp[k] = agg
		}
		if res.upperOn {
			agg.addUpper(res.upper)
		}
		if res.lowerOn {
			agg.addLower(res.lower)
		}
	}

	// Apply composed clamps. Each target policy's ControlKnob map is cloned at
	// most once, the first time one of its knobs is clamped; all of that policy's
	// clamped knobs then write into the same clone, so the input inner maps are
	// never mutated in place.
	clonedByPolicy := make(map[types.CPUProvisionPolicyName]types.ControlKnob)
	for k, agg := range clamp {
		rawItem := controlKnobs[k.policy][k.knob]
		if agg.conflict() {
			// Combined conflict: fail-open, emit one extra upper-metric observation.
			r.emitOne(metricControlKnobRestrictUpperBound, rawItem.Value,
				string(k.policy), restrictUnknownPolicy, k.knob,
				restrictModeEnforce, restrictActionConflictingBounds)
			continue
		}
		cloned, ok := clonedByPolicy[k.policy]
		if !ok {
			cloned = controlKnobs[k.policy].Clone()
			clonedByPolicy[k.policy] = cloned
		}
		v := rawItem.Value
		if agg.hasUpper {
			v = math.Min(v, agg.minUpper)
		}
		if agg.hasLower {
			v = math.Max(v, agg.maxLower)
		}
		cloned[k.knob] = types.ControlKnobItem{Value: v, Action: rawItem.Action}
	}
	for policy, cloned := range clonedByPolicy {
		controlKnobs[policy] = cloned
	}
	return controlKnobs
}

// evaluateBoundedConstraint resolves target/raw/ref, evaluates the upper/lower
// bounds, and selects the emit/aggregation outcome. It reads only the original
// controlKnobs map and never mutates it.
func (r *QoSRegionBase) evaluateBoundedConstraint(
	controlKnobs map[types.CPUProvisionPolicyName]types.ControlKnob,
	key compKey,
	c *configapi.ControlKnobConstraints,
) boundedConstraintResult {
	res := boundedConstraintResult{
		key:     key,
		enforce: c.Enforce != nil && *c.Enforce,
	}

	targetKnobs, targetOK := controlKnobs[key.target]
	rawItem, rawOK := targetKnobs[key.knob]
	refKnobs, refOK := controlKnobs[key.ref]
	refItem, refItemOK := refKnobs[key.knob]
	res.targetOK = targetOK
	res.refOK = refOK
	res.rawOK = rawOK
	if targetOK && rawOK {
		res.raw = rawItem.Value
	}

	if key.target == "" || key.ref == "" {
		res.errAction = restrictActionInvalidConfig
		general.Warningf("control knob restrict: invalid-config target %q ref %q knob %q (both must be explicit)", key.target, key.ref, key.knob)
		return res
	}

	// Ownership validation: a fully-explicit bounded constraint on a knob owned by
	// the exact path is invalid-config fail-open. This is the only bounded read of
	// the exact set; it does not alter exact-path execution.
	if r.ctrlKnobsNeedPolicyExactRestrict[key.knob] {
		res.errAction = restrictActionInvalidConfig
		general.Warningf("control knob restrict: invalid-config knob %q owned by exact path; bounded constraint ignored", key.knob)
		return res
	}

	// Target policy missing, or target policy present but its knob entry absent:
	// both are target-unavailable (the bounded rule cannot be evaluated against
	// the target side). The old split (missing policy -> unknown-policy, missing
	// knob -> passthrough) collapsed both onto one fail-open label.
	if !targetOK || !rawOK {
		res.errAction = restrictActionTargetUnavailable
		general.Warningf("control knob restrict: target-unavailable target %q knob %q (targetOK=%v rawOK=%v)", key.target, key.knob, targetOK, rawOK)
		return res
	}

	upperGap, upperRatio := 0.0, 0.0
	if c.MaxUpperGap != nil {
		upperGap = *c.MaxUpperGap
	}
	if c.MaxUpperGapRatio != nil {
		upperRatio = *c.MaxUpperGapRatio
	}
	lowerGap, lowerRatio := 0.0, 0.0
	if c.MaxLowerGap != nil {
		lowerGap = *c.MaxLowerGap
	}
	if c.MaxLowerGapRatio != nil {
		lowerRatio = *c.MaxLowerGapRatio
	}

	upper, lower, upperOn, lowerOn, upperOver, lowerOver, errAction := evaluateBounds(
		rawItem.Value, refItem.Value, upperGap, upperRatio, lowerGap, lowerRatio)
	res.upperOn, res.lowerOn = upperOn, lowerOn
	res.upper, res.lower = upper, lower
	res.upperOver, res.lowerOver = upperOver, lowerOver
	if !refOK || !refItemOK {
		res.errAction = restrictActionRefUnavailable
	} else {
		res.errAction = errAction
	}
	return res
}

// emitOne builds the fixed seven low-cardinality tags and stores one gauge
// observation. Upper and lower gauges share this exact key set; only the metric
// name and value differ. targetLabel/refLabel are already normalized ("unknown"
// when the policy was not present in the control-knob map).
func (r *QoSRegionBase) emitOne(metricName string, value float64,
	targetLabel, refLabel string, knob configapi.ControlKnobName,
	mode, action string,
) {
	tags := []metrics.MetricTag{
		{Key: metricTagKeyRegionType, Val: string(r.regionType)},
		{Key: metricTagKeyRegionName, Val: r.name},
		{Key: "target_policy", Val: targetLabel},
		{Key: "reference_policy", Val: refLabel},
		{Key: "control_knob", Val: string(knob)},
		{Key: "mode", Val: mode},
		{Key: "action", Val: action},
	}
	storeErr := r.emitter.StoreFloat64(metricName, value, metrics.MetricTypeNameRaw, tags...)
	// Diagnostic trace: one structured line per gauge observation. All fields are
	// the already-low-cardinality tag set plus the store result; no credentials.
	general.InfoS("bounded restrict emit",
		"metricName", metricName,
		"value", value,
		"regionType", string(r.regionType),
		"regionName", r.name,
		"targetPolicy", targetLabel,
		"referencePolicy", refLabel,
		"controlKnob", string(knob),
		"mode", mode,
		"action", action,
		"storeError", storeErr,
	)
}

// emitBoundedResult emits low-cardinality observations for one evaluated
// constraint. On fail-open paths (errAction set) it emits exactly one upper
// gauge with raw. On success it emits the upper gauge iff upperOn and the lower
// gauge iff lowerOn, with the bound as the value; observed+enforce collapses
// to clamped.
func (r *QoSRegionBase) emitBoundedResult(res boundedConstraintResult) {
	mode := restrictModeShadow
	if res.enforce {
		mode = restrictModeEnforce
	}
	targetLabel := restrictUnknownPolicy
	if res.targetOK {
		targetLabel = string(res.key.target)
	}
	refLabel := restrictUnknownPolicy
	if res.refOK {
		refLabel = string(res.key.ref)
	}
	// The control_knob tag is normalized like the policy tags: it names the knob
	// only when the target policy and its raw knob actually exist in the map;
	// otherwise it is "unknown" so a typo target never leaks a configured knob
	// string into the label set.
	knobLabel := configapi.ControlKnobName(restrictUnknownPolicy)
	if res.targetOK && res.rawOK {
		knobLabel = res.key.knob
	}
	// per-side action: overshoot collapses observed -> clamped when enforce.
	sideAction := func(overshoot bool) string {
		if !overshoot {
			return restrictActionPassthrough
		}
		if res.enforce {
			return restrictActionClamped
		}
		return restrictActionObserved
	}
	if res.errAction != "" {
		r.emitOne(metricControlKnobRestrictUpperBound, res.raw,
			targetLabel, refLabel, knobLabel, mode, res.errAction)
		return
	}
	if res.upperOn {
		r.emitOne(metricControlKnobRestrictUpperBound, res.upper,
			targetLabel, refLabel, knobLabel, mode, sideAction(res.upperOver))
	}
	if res.lowerOn {
		r.emitOne(metricControlKnobRestrictLowerBound, res.lower,
			targetLabel, refLabel, knobLabel, mode, sideAction(res.lowerOver))
	}
}
