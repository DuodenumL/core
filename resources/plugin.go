package resources

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"time"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources/types"
	coretypes "github.com/projecteru2/core/types"
)

const (
	// Incr increase
	Incr = true

	// Decr decrease
	Decr = false

	getNodesCapacityCommand           = "get-capacity"
	getNodeResourceInfoCommand        = "get-node"
	setNodeResourceInfoCommand        = "set-node"
	allocCommand                      = "alloc"
	reallocCommand                    = "realloc"
	remapCommand                      = "remap"
	updateNodeResourceUsageCommand    = "update-usage"
	updateNodeResourceCapacityCommand = "update-capacity"
	addNodeCommand                    = "add-node"
	removeNodeCommand                 = "remove-node"
)

// Plugin resource plugin
type Plugin interface {
	// GetNodesCapacity returns available nodes and total capacity
	GetNodesCapacity(ctx context.Context, nodes []string, resourceOpts coretypes.WorkloadResourceOpts) (capacityInfo map[string]*types.NodeCapacityInfo, total int, err error)

	// GetNodeResourceInfo returns total resource info and available resource info of the node, format: {"cpu": 2}
	// also returns diffs, format: ["node.VolumeUsed != sum(workload.VolumeRequest"]
	GetNodeResourceInfo(ctx context.Context, node string, workloads []*coretypes.Workload, fix bool) (resourceCapacity coretypes.NodeResourceArgs, resourceUsage coretypes.NodeResourceArgs, diffs []string, err error)

	// SetNodeResourceInfo sets both total node resource info and allocated resource info
	// used for rollback of RemoveNode
	// notice: here uses absolute values, not delta values
	SetNodeResourceInfo(ctx context.Context, node string, resourceCapacity coretypes.NodeResourceArgs, resourceUsage coretypes.NodeResourceArgs) error

	// Alloc allocates resource, returns engine args for each workload, format: [{"cpus": 1.2}, {"cpus": 1.2}]
	// also returns resource args for each workload, format: [{"cpus": 1.2}, {"cpus": 1.2}]
	// pure calculation
	Alloc(ctx context.Context, node string, deployCount int, resourceOpts coretypes.WorkloadResourceOpts) ([]coretypes.EngineArgs, []coretypes.WorkloadResourceArgs, error)

	// Realloc reallocates resource, returns engine args, delta resource args and final resource args.
	// should return error if resource of some node is not enough for the realloc operation.
	// pure calculation
	Realloc(ctx context.Context, node string, originResourceArgs coretypes.WorkloadResourceArgs, resourceOpts coretypes.WorkloadResourceOpts) (engineArgs coretypes.EngineArgs, deltaResourceArgs coretypes.WorkloadResourceArgs, finalResourceArgs coretypes.WorkloadResourceArgs, err error)
	//Realloc2(ctx context.Context, workloads []*coretypes.Workload, resourceOpts coretypes.RawParams) (map[string]coretypes.RawParams, map[string]coretypes.RawParams, error)

	// Remap remaps resources based on workload metadata and node resource usage, then returns engine args for workloads.
	// pure calculation
	Remap(ctx context.Context, node string, workloadMap map[string]*coretypes.Workload) (map[string]coretypes.EngineArgs, error)

	// UpdateNodeResourceUsage updates node resource usage
	UpdateNodeResourceUsage(ctx context.Context, node string, resourceArgs []coretypes.WorkloadResourceArgs, incr bool) error

	// UpdateNodeResourceCapacity updates node resource capacity
	UpdateNodeResourceCapacity(ctx context.Context, node string, resourceOpts coretypes.NodeResourceOpts, incr bool) error

	// AddNode adds a node with requested resource, returns resource capacity and (empty) resource usage
	// should return error if the node already exists
	AddNode(ctx context.Context, node string, resourceOpts coretypes.NodeResourceOpts) (resourceCapacity coretypes.NodeResourceArgs, resourceUsage coretypes.NodeResourceArgs, err error)

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

func (bp *BinaryPlugin) getArgs(req interface{}) []string {
	t := reflect.TypeOf(req)
	if t.Kind() != reflect.Struct {
		return nil
	}
	v := reflect.ValueOf(req)
	args := []string{}

	for i := 0; i < t.NumField(); i++ {
		fieldType := t.Field(i).Type
		fieldValue := v.Field(i).Interface()
		jsonTag := t.Field(i).Tag.Get("json")

		switch {
		case fieldType.Kind() == reflect.Map:
			body, err := json.Marshal(fieldValue)
			if err != nil {
				break
			}
			args = append(args, "--"+jsonTag, string(body))
		case fieldType.Kind() == reflect.Slice:
			for j := 0; j < v.Field(i).Len(); j++ {
				if v.Field(i).Index(j).Kind() == reflect.Map {
					body, err := json.Marshal(v.Field(i).Index(j).Interface())
					if err != nil {
						break
					}
					args = append(args, "--"+jsonTag, string(body))
				} else {
					args = append(args, "--"+jsonTag, fmt.Sprintf("%v", v.Field(i).Index(j).Interface()))
				}
			}
		case fieldType.Kind() == reflect.Bool:
			if fieldValue.(bool) {
				args = append(args, "--"+jsonTag)
			}
		default:
			args = append(args, "--"+jsonTag, fmt.Sprintf("%v", fieldValue))
		}
	}
	return args
}

// call calls plugin and gets json response
func (bp *BinaryPlugin) call(ctx context.Context, cmd string, req interface{}, resp interface{}, timeout time.Duration) error {
	if timeout == 0 {
		timeout = bp.timeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	args := bp.getArgs(req)
	args = append([]string{cmd}, args...)
	command := exec.CommandContext(ctx, bp.path, args...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr

	log.Infof(ctx, "[callBinaryPlugin] command: %s %s", bp.path, strings.Join(args, " "))
	if err := command.Run(); err != nil {
		log.Errorf(ctx, "[callBinaryPlugin] failed to run plugin %s, command %v, err %s", bp.path, args, err)
		return err
	}
	log.Infof(ctx, "[callBinaryPlugin] log from plugin %s: %s", bp.path, stderr.String())

	stdoutBytes := stdout.Bytes()
	log.Debugf(ctx, "[callBinaryPlugin] output from plugin %s: %s", bp.path, string(stdoutBytes))
	if len(stdoutBytes) == 0 {
		stdoutBytes = []byte("{}")
	}
	if err := json.Unmarshal(stdoutBytes, resp); err != nil {
		log.Errorf(ctx, "[callBinaryPlugin] failed to unmarshal output of plugin %s, command %v, output %s, err %s", bp.path, args, string(stdoutBytes), err)
		return err
	}
	return nil
}

// GetNodesCapacity .
func (bp *BinaryPlugin) GetNodesCapacity(ctx context.Context, nodes []string, resourceOpts coretypes.WorkloadResourceOpts) (capacityInfo map[string]*types.NodeCapacityInfo, total int, err error) {
	req := types.GetNodesCapacityRequest{
		NodeNames:    nodes,
		ResourceOpts: resourceOpts,
	}
	resp := &types.GetNodesCapacityResponse{}
	err = bp.call(ctx, getNodesCapacityCommand, req, resp, bp.timeout)
	return resp.Nodes, resp.Total, err
}

// GetNodeResourceInfo .
func (bp *BinaryPlugin) GetNodeResourceInfo(ctx context.Context, node string, workloads []*coretypes.Workload, fix bool) (resourceCapacity coretypes.NodeResourceArgs, resourceUsage coretypes.NodeResourceArgs, diffs []string, err error) {
	workloadMap := map[string]coretypes.WorkloadResourceArgs{}
	for _, workload := range workloads {
		workloadMap[workload.ID] = workload.ResourceArgs[bp.Name()]
	}

	req := types.GetNodeResourceInfoRequest{
		NodeName:    node,
		WorkloadMap: workloadMap,
		Fix:         false,
	}
	resp := &types.GetNodeResourceInfoResponse{}
	if err = bp.call(ctx, getNodeResourceInfoCommand, req, resp, bp.timeout); err != nil {
		return nil, nil, nil, err
	}
	return resp.ResourceInfo.Capacity, resp.ResourceInfo.Usage, diffs, nil
}

// SetNodeResourceInfo .
func (bp *BinaryPlugin) SetNodeResourceInfo(ctx context.Context, node string, resourceCapacity coretypes.NodeResourceArgs, resourceUsage coretypes.NodeResourceArgs) error {
	req := types.SetNodeResourceInfoRequest{
		NodeName: node,
		Capacity: resourceCapacity,
		Usage:    resourceUsage,
	}
	resp := &types.SetNodeResourceInfoResponse{}
	return bp.call(ctx, setNodeResourceInfoCommand, req, resp, bp.timeout)
}

// Alloc .
func (bp *BinaryPlugin) Alloc(ctx context.Context, node string, deployCount int, resourceOpts coretypes.WorkloadResourceOpts) ([]coretypes.EngineArgs, []coretypes.WorkloadResourceArgs, error) {
	req := types.AllocRequest{
		NodeName:     node,
		DeployCount:  deployCount,
		ResourceOpts: resourceOpts,
	}
	resp := &types.AllocResponse{}
	if err := bp.call(ctx, allocCommand, req, resp, bp.timeout); err != nil {
		return nil, nil, err
	}
	return resp.EngineArgs, resp.ResourceArgs, nil
}

// Realloc .
func (bp *BinaryPlugin) Realloc(ctx context.Context, node string, originResourceArgs coretypes.WorkloadResourceArgs, resourceOpts coretypes.WorkloadResourceOpts) (engineArgs coretypes.EngineArgs, deltaResourceArgs coretypes.WorkloadResourceArgs, finalResourceArgs coretypes.WorkloadResourceArgs, err error) {
	req := types.ReallocRequest{
		NodeName:     node,
		Old:          originResourceArgs,
		ResourceOpts: resourceOpts,
	}
	resp := &types.ReallocResponse{}
	if err := bp.call(ctx, reallocCommand, req, resp, bp.timeout); err != nil {
		return nil, nil, nil, err
	}
	return resp.EngineArgs, resp.Delta, resp.ResourceArgs, nil
}

// Remap .
func (bp *BinaryPlugin) Remap(ctx context.Context, node string, workloadMap map[string]*coretypes.Workload) (map[string]coretypes.EngineArgs, error) {
	workloadResourceArgsMap := map[string]coretypes.WorkloadResourceArgs{}
	for workloadID, workload := range workloadMap {
		workloadResourceArgsMap[workloadID] = workload.ResourceArgs[bp.Name()]
	}

	req := types.RemapRequest{
		NodeName:    node,
		WorkloadMap: workloadResourceArgsMap,
	}
	resp := &types.RemapResponse{}
	if err := bp.call(ctx, remapCommand, req, resp, bp.timeout); err != nil {
		return nil, err
	}
	return resp.EngineArgsMap, nil
}

// UpdateNodeResourceUsage .
func (bp *BinaryPlugin) UpdateNodeResourceUsage(ctx context.Context, node string, resourceArgs []coretypes.WorkloadResourceArgs, incr bool) error {
	req := types.UpdateNodeResourceUsageRequest{
		NodeName:     node,
		ResourceArgs: resourceArgs,
		Decr:         !incr,
	}
	resp := &types.UpdateNodeResourceUsageResponse{}
	return bp.call(ctx, updateNodeResourceUsageCommand, req, resp, bp.timeout)
}

// UpdateNodeResourceCapacity ,
func (bp *BinaryPlugin) UpdateNodeResourceCapacity(ctx context.Context, node string, resourceOpts coretypes.NodeResourceOpts, incr bool) error {
	req := types.UpdateNodeResourceCapacityRequest{
		NodeName:     node,
		ResourceOpts: resourceOpts,
		Decr:         !incr,
	}
	resp := &types.UpdateNodeResourceCapacityResponse{}
	return bp.call(ctx, updateNodeResourceCapacityCommand, req, resp, bp.timeout)
}

// AddNode .
func (bp *BinaryPlugin) AddNode(ctx context.Context, node string, resourceOpts coretypes.NodeResourceOpts) (resourceCapacity coretypes.NodeResourceArgs, resourceUsage coretypes.NodeResourceArgs, err error) {
	req := types.AddNodeRequest{
		NodeName:     node,
		ResourceOpts: resourceOpts,
	}
	resp := &types.AddNodeResponse{}
	if err := bp.call(ctx, addNodeCommand, req, resp, bp.timeout); err != nil {
		return nil, nil, err
	}
	return resp.Capacity, resp.Usage, nil
}

// RemoveNode .
func (bp *BinaryPlugin) RemoveNode(ctx context.Context, node string) error {
	req := types.RemoveNodeRequest{
		NodeName: node,
	}
	resp := &types.RemoveNodeResponse{}
	return bp.call(ctx, removeNodeCommand, req, resp, bp.timeout)
}

// Name .
func (bp *BinaryPlugin) Name() string {
	return bp.path
}
