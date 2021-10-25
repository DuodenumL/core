package types

// NodeResourceInfo .
type NodeResourceInfo struct {
	NodeName string
	Capacity int

	// Usage current resource usage
	Usage float64
	// Rate proportion of requested resources to total
	Rate float64

	// Weight used for weighted average
	Weight float64
}
