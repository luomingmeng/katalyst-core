// Package deadlock detects cyclic cpuset partition relationships before a plan
// is applied, turning a runtime deadlock into a deliberate early error.
package deadlock
