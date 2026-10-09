// Package bulkhead implements the bulkhead CPU isolation manager: it owns the
// per-round cpuset adjustment loop, the applied-view publication boundary, and
// the plugin registry lifecycle.
//
// Invariant: the manager is the single writer of the shared applied view;
// plugins return typed outcomes (api.TopologyOutcome) but never mutate shared
// state directly.
//
// Constraint: this package must not import the topology algorithm
// (utils/topology) or any concrete plugin; it depends only on api + model and
// dispatches plugins by interface assertion (see api/api_boundary_test.go).
package bulkhead
