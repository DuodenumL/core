package resources

import (
	"context"
	"encoding/json"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources/types"
	coretypes "github.com/projecteru2/core/types"
	"os/exec"
	"time"
)

type RawParams map[string][]string

// Plugin resource plugin
type Plugin interface {
	// LockNodes locks the given nodes
	LockNodes(ctx context.Context, nodes []string) error

	// UnlockNodes unlocks the given nodes
	UnlockNodes(ctx context.Context, nodes []string) error

	// GetAvailableNodes returns available nodes and total capacity
	GetAvailableNodes(ctx context.Context, rawRequest RawParams) (map[string]*types.NodeResourceInfo, int, error)

	// SetNodeResource sets the node's resource info
	SetNodeResource(ctx context.Context, node string, rawRequest RawParams) error

	// GetNodesResource returns the resource info of given nodes, format: {"node1": {"cpu": "4.00 / 2"}}
	GetNodesResource(ctx context.Context, nodes []string) (map[string]RawParams, error)

	// Alloc allocates resource, returns engine args for each workload, format: [{"cpus": ["2"]}, {"cpus": ["2"]}]
	// also returns resource args for each workload, format: [{"cpus": ["2"]}, {"cpus": ["2"]}]
	Alloc(ctx context.Context, node string, deployCount int, rawRequest RawParams) ([]RawParams, []RawParams, error)

	// Remap remaps resources based on workload metadata and node resource usage, then returns engine args for workloads.
	Remap(ctx context.Context, node string, workloadMap map[string]*coretypes.Workload) (map[string]RawParams, error)

	// Rollback rollbacks resource
	Rollback(ctx context.Context, node string, resourceArgs []RawParams) error

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

// GetAvailableNodes .
func (bp *BinaryPlugin) GetAvailableNodes(ctx context.Context, rawRequest RawParams) (map[string]*types.NodeResourceInfo, int, error) {
	panic("implement me")
}

// GetNodesResource .
func (bp *BinaryPlugin) GetNodesResource(ctx context.Context, nodes []string) (map[string]RawParams, error) {
	panic("implement me")
}

// Alloc .
func (bp *BinaryPlugin) Alloc(ctx context.Context, node string, deployCount int, rawRequest RawParams) ([]RawParams, []RawParams, error) {
	panic("implement me")
}

// Rollback .
func (bp *BinaryPlugin) Rollback(ctx context.Context, node string, resourceArgs []RawParams) error {
	panic("implement me")
}

// Remap .
func (bp *BinaryPlugin) Remap(ctx context.Context, node string, workloadMap map[string]*coretypes.Workload) (map[string]RawParams, error) {
	panic("implement me")
}

// SetNodeResource .
func (bp *BinaryPlugin) SetNodeResource(ctx context.Context, node string, rawRequest RawParams) error {
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
