// Package registry owns plugin registration and ordering.
//
// Why: registration-time validation (e.g. at most one TopologyPlugin) fails
// fast at startup instead of silently picking first-wins.
package registry
