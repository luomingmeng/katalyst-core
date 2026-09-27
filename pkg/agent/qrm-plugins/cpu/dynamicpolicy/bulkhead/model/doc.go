// Package model holds the data structures shared across the bulkhead layers:
// AppliedView, DesiredView, and partition views.
//
// Invariant: model has no logic and imports no bulkhead package; it is the
// leaf of the dependency graph.
package model
