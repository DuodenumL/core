package types

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

type GetNodeResourceInfoRequest struct {
	NodeName    string                                `json:"node"`
	WorkloadMap map[string]types.WorkloadResourceArgs `json:"workload-map"`
	Fix         bool                                  `json:"fix"`
}

type GetNodeResourceInfoResponse struct {
	ResourceInfo NodeResourceInfo `json:"resource_info"`
	Diffs        []string         `json:"diffs"`
}

type SetNodeResourceInfoRequest struct {
	NodeName string                 `json:"node"`
	Capacity types.NodeResourceArgs `json:"capacity"`
	Usage    types.NodeResourceArgs `json:"usage"`
}

type SetNodeResourceInfoResponse struct{}

type AllocRequest struct {
	NodeName     string                     `json:"node"`
	DeployCount  int                        `json:"deploy"`
	ResourceOpts types.WorkloadResourceOpts `json:"resource-opts"`
}

type AllocResponse struct {
	EngineArgs   []types.EngineArgs           `json:"engine_args"`
	ResourceArgs []types.WorkloadResourceArgs `json:"resource_args"`
}

type ReallocRequest struct {
	NodeName     string                     `json:"node"`
	Old          types.WorkloadResourceArgs `json:"old"`
	ResourceOpts types.WorkloadResourceOpts `json:"resource-opts"`
}

type ReallocResponse struct {
	EngineArgs   types.EngineArgs           `json:"engine_args"`
	Delta        types.WorkloadResourceArgs `json:"delta"`
	ResourceArgs types.WorkloadResourceArgs `json:"resource_args"`
}

type RemapRequest struct {
	NodeName    string                                `json:"node"`
	WorkloadMap map[string]types.WorkloadResourceArgs `json:"workload-map"`
}

type RemapResponse struct {
	EngineArgsMap map[string]types.EngineArgs `json:"engine_args_map"`
}

type UpdateNodeResourceUsageRequest struct {
	NodeName     string                       `json:"node"`
	ResourceArgs []types.WorkloadResourceArgs `json:"resource-args"`
	Decr         bool                         `json:"decr"`
}

type UpdateNodeResourceUsageResponse struct{}

type UpdateNodeResourceCapacityRequest struct {
	NodeName     string                 `json:"node"`
	ResourceOpts types.NodeResourceOpts `json:"resource-opts"`
	Decr         bool                   `json:"decr"`
	Delta        bool                   `json:"delta"`
}

type UpdateNodeResourceCapacityResponse struct{}

type AddNodeRequest struct {
	NodeName     string                 `json:"node"`
	ResourceOpts types.NodeResourceOpts `json:"resource-opts"`
}

type AddNodeResponse struct {
	Capacity types.NodeResourceArgs `json:"capacity"`
	Usage    types.NodeResourceArgs `json:"usage"`
}

type RemoveNodeRequest struct {
	NodeName string `json:"node"`
}

type RemoveNodeResponse struct{}
