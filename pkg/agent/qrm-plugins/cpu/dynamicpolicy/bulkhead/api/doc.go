// Package api defines the opaque contract between the bulkhead manager and its
// plugins. It contains only behavior interfaces, convergence levels, and the
// published result structs.
//
// Constraint: this package must not import utils/topology or any plugin. The
// dependency direction is registry/manager -> api -> model only. Enforced by
// api/api_boundary_test.go.
package api
