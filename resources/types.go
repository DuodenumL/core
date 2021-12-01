package resources

import "github.com/projecteru2/core/types"

// NodeCapacityInfo .
type NodeCapacityInfo struct {
	NodeName string
	Capacity int

	// Usage current resource usage
	Usage float64
	// Rate proportion of requested resources to total
	Rate float64

	// Weight used for weighted average
	Weight float64
}

type NodeResourceInfo struct {
	Capacity types.NodeResourceArgs `json:"capacity"`
	Usage    types.NodeResourceArgs `json:"usage"`
}

// GetNodesCapacityRequest .
type GetNodesCapacityRequest struct {
	NodeNames    []string                   `json:"node"`
	ResourceOpts types.WorkloadResourceOpts `json:"resource-opts"`
}

// GetNodesCapacityResponse .
type GetNodesCapacityResponse struct {
	Nodes map[string]*NodeCapacityInfo `json:"nodes"`
	Total int                          `json:"total"`
}

// GetNodeResourceInfoRequest .
type GetNodeResourceInfoRequest struct {
	NodeName    string                                `json:"node"`
	WorkloadMap map[string]types.WorkloadResourceArgs `json:"workload-map"`
	Fix         bool                                  `json:"fix"`
}

// GetNodeResourceInfoResponse ,
type GetNodeResourceInfoResponse struct {
	ResourceInfo *NodeResourceInfo `json:"resource_info"`
	Diffs        []string          `json:"diffs"`
}

// SetNodeResourceInfoRequest .
type SetNodeResourceInfoRequest struct {
	NodeName string                 `json:"node"`
	Capacity types.NodeResourceArgs `json:"capacity"`
	Usage    types.NodeResourceArgs `json:"usage"`
}

// SetNodeResourceInfoResponse .
type SetNodeResourceInfoResponse struct{}

// AllocRequest .
type AllocRequest struct {
	NodeName     string                     `json:"node"`
	DeployCount  int                        `json:"deploy"`
	ResourceOpts types.WorkloadResourceOpts `json:"resource-opts"`
}

// AllocResponse .
type AllocResponse struct {
	EngineArgs   []types.EngineArgs           `json:"engine_args"`
	ResourceArgs []types.WorkloadResourceArgs `json:"resource_args"`
}

// ReallocRequest .
type ReallocRequest struct {
	NodeName     string                     `json:"node"`
	Old          types.WorkloadResourceArgs `json:"old"`
	ResourceOpts types.WorkloadResourceOpts `json:"resource-opts"`
}

// ReallocResponse .
type ReallocResponse struct {
	EngineArgs   types.EngineArgs           `json:"engine_args"`
	Delta        types.WorkloadResourceArgs `json:"delta"`
	ResourceArgs types.WorkloadResourceArgs `json:"resource_args"`
}

// RemapRequest .
type RemapRequest struct {
	NodeName    string                                `json:"node"`
	WorkloadMap map[string]types.WorkloadResourceArgs `json:"workload-map"`
}

// RemapResponse .
type RemapResponse struct {
	EngineArgsMap map[string]types.EngineArgs `json:"engine_args_map"`
}

// UpdateNodeResourceUsageRequest .
type UpdateNodeResourceUsageRequest struct {
	NodeName     string                       `json:"node"`
	ResourceArgs []types.WorkloadResourceArgs `json:"resource-args"`
	Decr         bool                         `json:"decr"`
}

// UpdateNodeResourceUsageResponse .
type UpdateNodeResourceUsageResponse struct{}

// UpdateNodeResourceCapacityRequest .
type UpdateNodeResourceCapacityRequest struct {
	NodeName     string                 `json:"node"`
	ResourceOpts types.NodeResourceOpts `json:"resource-opts"`
	Decr         bool                   `json:"decr"`
	Delta        bool                   `json:"delta"`
}

// UpdateNodeResourceCapacityResponse .
type UpdateNodeResourceCapacityResponse struct{}

// AddNodeRequest .
type AddNodeRequest struct {
	NodeName     string                 `json:"node"`
	ResourceOpts types.NodeResourceOpts `json:"resource-opts"`
}

// AddNodeResponse .
type AddNodeResponse struct {
	Capacity types.NodeResourceArgs `json:"capacity"`
	Usage    types.NodeResourceArgs `json:"usage"`
}

// RemoveNodeRequest .
type RemoveNodeRequest struct {
	NodeName string `json:"node"`
}

// RemoveNodeResponse .
type RemoveNodeResponse struct{}

// GetMostIdleNodeRequest .
type GetMostIdleNodeRequest struct {
	NodeNames []string `json:"node"`
}

// GetMostIdleNodeResponse .
type GetMostIdleNodeResponse struct {
	NodeName string `json:"node"`
	Priority int    `json:"priority"`
}
