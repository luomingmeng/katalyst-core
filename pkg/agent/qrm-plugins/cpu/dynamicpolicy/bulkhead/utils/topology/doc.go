// Package topology implements the cpuset partition DAG planner and convergence
// coordinator. It is a pure function of (desired view, applied view, CPU
// inventory); it holds no manager/plugin state.
//
// Performance: planning at 10k nodes targets <1s and ~810K allocs/op; hot paths
// reuse precomputed childRel paths and pooled objects.
//
// Boundary: buildPlanOperations must fall back deterministically when childRels
// is uninitialized (see TestBuildPlanOperationsNilChildRelsByRelFallback).
package topology
