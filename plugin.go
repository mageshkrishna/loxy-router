package main

// SchedulerPlugin lets users inject custom routing logic without forking.
//
// Usage: implement this interface and pass it to NewScheduler().
// If Pick() returns ("", nil) the built-in scheduler takes over.
//
// Example — always route GPU-heavy models to a specific backend:
//
//	type GPURouter struct{}
//	func (g *GPURouter) Pick(model string, backends []*Backend) (string, error) {
//	    if model == "llama3:70b" {
//	        return "gpu-box-1", nil
//	    }
//	    return "", nil // fall through to default scheduler
//	}
type SchedulerPlugin interface {
	// Pick returns the name of the backend to route to, or "" to fall through
	// to the built-in scheduler. Return a non-nil error to reject the request.
	Pick(model string, backends []*Backend) (backendName string, err error)
}
