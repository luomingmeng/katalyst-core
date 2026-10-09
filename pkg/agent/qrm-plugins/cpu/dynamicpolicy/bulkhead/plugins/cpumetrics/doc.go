// Package cpumetrics samples per-pod/per-NUMA CPU metrics on the hot path.
//
// Performance: allocations are minimized (numaBuckets, label projection) and
// pooled objects reset in a known order. Do not add per-pod map/string work in
// the sample loop without measuring.
package cpumetrics
