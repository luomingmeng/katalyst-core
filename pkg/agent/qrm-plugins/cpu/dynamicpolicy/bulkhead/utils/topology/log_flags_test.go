package topology

import "k8s.io/klog/v2"

// Registration only: verbosity is set in an isolated test process by CLI.
func init() { klog.InitFlags(nil) }
