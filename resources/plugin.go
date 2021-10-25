package resources

import (
	"context"
	"encoding/json"
	"os/exec"
	"time"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources/types"
	coretypes "github.com/projecteru2/core/types"
)


// Incr increase
const Incr = true

// Decr decrease
const Decr = false

// Plugin resource plugin
type Plugin interface {
	// LockNodes locks the given nodes
	LockNodes(ctx context.Context, nodes []string) error

	// UnlockNodes unlocks the given nodes
	UnlockNodes(ctx context.Context, nodes []string) error

	// SelectAvailableNodes returns available nodes and total capacity
	SelectAvailableNodes(ctx context.Context, nodes []string, resourceOpts coretypes.RawParams) (map[string]*types.NodeResourceInfo, int, error)

	// GetNodeResourceInfo returns total resource info and available resource info of the node, format: {"cpu": 2}
	// also returns diffs, format: ["node.VolumeUsed != sum(workload.VolumeRequest"]
	GetNodeResourceInfo(ctx context.Context, node string, workloads []*coretypes.Workload, fix bool) (coretypes.RawParams, coretypes.RawParams, []string, error)

	// SetNodeResourceInfo sets both total node resource info and allocated resource info
	// used for rollback of RemoveNode
	// notice: here uses absolute values, not delta values
	SetNodeResourceInfo(ctx context.Context, resourceCapacity coretypes.RawParams, resourceUsage coretypes.RawParams) error

	// Alloc allocates resource, returns engine args for each workload, format: [{"cpus": 1.2}, {"cpus": 1.2}]
	// also returns resource args for each workload, format: [{"cpus": 1.2}, {"cpus": 1.2}]
	// pure calculation
	Alloc(ctx context.Context, node string, deployCount int, resourceOpts coretypes.RawParams) ([]coretypes.RawParams, []coretypes.RawParams, error)

	// Realloc reallocates resource, returns engine args and resource args for each workload.
	// should return error if resource of some node is not enough for the realloc operation.
	// pure calculation
	Realloc(ctx context.Context, workloads []*coretypes.Workload, resourceOpts coretypes.RawParams) (map[string]coretypes.RawParams, map[string]coretypes.RawParams, error)

	// Remap remaps resources based on workload metadata and node resource usage, then returns engine args for workloads.
	// pure calculation
	Remap(ctx context.Context, node string, workloadMap map[string]*coretypes.Workload) (map[string]coretypes.RawParams, error)

	// Diff returns dstResourceArgs - srcResourceArgs
	// e.g.: src: {"cpu": 1.2}, dst: {"cpu": 1.5}, result: {"cpu": 0.3}
	// pure calculation
	Diff(ctx context.Context, srcResourceArgs coretypes.RawParams, dstResourceArgs coretypes.RawParams) (coretypes.RawParams, error)

	// UpdateNodeResourceUsage updates node resource usage
	UpdateNodeResourceUsage(ctx context.Context, node string, resourceArgs []coretypes.RawParams, direction bool) error

	// UpdateNodeResourceCapacity updates node resource capacity
	UpdateNodeResourceCapacity(ctx context.Context, node string, resourceOpts coretypes.RawParams, direction bool) error

	// AddNode adds a node with requested resource, returns resource capacity and (empty) resource usage
	// should return error if the node already exists
	AddNode(ctx context.Context, node string, resourceOpts coretypes.RawParams) (coretypes.RawParams, coretypes.RawParams, error)

	// RemoveNode removes node
	RemoveNode(ctx context.Context, node string) error

	// Name returns the name of plugin
	Name() string
}

// BinaryPlugin .
type BinaryPlugin struct {
	path    string
	timeout time.Duration
}

// call calls plugin and gets json response
func (bp *BinaryPlugin) call(ctx context.Context, resp interface{}, timeout time.Duration, cmd string, args ...string) error {
	if timeout == 0 {
		timeout = bp.timeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	command := exec.CommandContext(ctx, cmd, args...)
	if err := command.Run(); err != nil {
		log.Errorf(ctx, "[callBinaryPlugin] failed to run plugin %s, command %s %v, err %s", bp.path, cmd, args, err)
		return err
	}
	output, err := command.Output()
	if err != nil {
		log.Errorf(ctx, "[callBinaryPlugin] failed to get output of plugin %s, command %s %v, err %s", bp.path, cmd, args, err)
		return err
	}
	if json.Unmarshal(output, resp) != nil {
		log.Errorf(ctx, "[callBinaryPlugin] failed to unmarshal output of plugin %s, command %s %v, err %s", bp.path, cmd, args, err)
		return err
	}
	return nil
}

// LockNodes .
func (bp *BinaryPlugin) LockNodes(ctx context.Context, nodes []string) (err error) {
	panic("implement me")
}

// UnlockNodes .
func (bp *BinaryPlugin) UnlockNodes(ctx context.Context, nodes []string) (err error) {
	panic("implement me")
}

// SelectAvailableNodes .
func (bp *BinaryPlugin) SelectAvailableNodes(ctx context.Context, nodes []string, resourceOpts coretypes.RawParams) (map[string]*types.NodeResourceInfo, int, error) {
	panic("implement me")
}

// GetNodeResourceInfo .
func (bp *BinaryPlugin) GetNodeResourceInfo(ctx context.Context, node string, workloads []*coretypes.Workload, fix bool) (coretypes.RawParams, coretypes.RawParams, []string, error) {
	panic("implement me")
}

// SetNodeResourceInfo .
func (bp *BinaryPlugin) SetNodeResourceInfo(ctx context.Context, resourceCapacity coretypes.RawParams, resourceUsage coretypes.RawParams) error {
	panic("implement me")
}

// Alloc .
func (bp *BinaryPlugin) Alloc(ctx context.Context, node string, deployCount int, resourceOpts coretypes.RawParams) ([]coretypes.RawParams, []coretypes.RawParams, error) {
	panic("implement me")
}

func (bp *BinaryPlugin) Realloc(ctx context.Context, workloads []*coretypes.Workload, resourceOpts coretypes.RawParams) (map[string]coretypes.RawParams, map[string]coretypes.RawParams, error) {
	panic("implement me")
}

// UpdateNodeResourceUsage .
func (bp *BinaryPlugin) UpdateNodeResourceUsage(ctx context.Context, node string, resourceArgs []coretypes.RawParams, direction bool) error {
	panic("implement me")
}

// UpdateNodeResourceCapacity .
func (bp *BinaryPlugin) UpdateNodeResourceCapacity(ctx context.Context, node string, resourceOpts coretypes.RawParams, direction bool) error {
	panic("implement me")
}

// Diff .
func (bp *BinaryPlugin) Diff(ctx context.Context, srcResourceArgs coretypes.RawParams, dstResourceArgs coretypes.RawParams) (coretypes.RawParams, error) {
	panic("implement me")
}

// Remap .
func (bp *BinaryPlugin) Remap(ctx context.Context, node string, workloadMap map[string]*coretypes.Workload) (map[string]coretypes.RawParams, error) {
	panic("implement me")
}

// AddNode .
func (bp *BinaryPlugin) AddNode(ctx context.Context, node string, resourceOpts coretypes.RawParams) (coretypes.RawParams, coretypes.RawParams, error) {
	panic("implement me")
}

// RemoveNode .
func (bp *BinaryPlugin) RemoveNode(ctx context.Context, node string) error {
	panic("implement me")
}

// Name .
func (bp *BinaryPlugin) Name() string {
	return bp.path
}
