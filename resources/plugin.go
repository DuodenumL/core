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
	GetNodesCapacity(ctx context.Context, nodeNames []string, resourceOpts coretypes.WorkloadResourceOpts) (*GetNodesCapacityResponse, error)

	// GetNodeResourceInfo returns total resource info and available resource info of the nodeName, format: {"cpu": 2}
	// also returns diffs, format: ["node.VolumeUsed != sum(workload.VolumeRequest"]
	GetNodeResourceInfo(ctx context.Context, nodeName string, workloads []*coretypes.Workload, fix bool) (*GetNodeResourceInfoResponse, error)

	// SetNodeResourceInfo sets both total node resource info and allocated resource info
	// used for rollback of RemoveNode
	// notice: here uses absolute values, not delta values
	SetNodeResourceInfo(ctx context.Context, nodeName string, resourceCapacity coretypes.NodeResourceArgs, resourceUsage coretypes.NodeResourceArgs) (*SetNodeResourceInfoResponse, error)

	// Alloc allocates resource, returns engine args for each workload, format: [{"cpus": 1.2}, {"cpus": 1.2}]
	// also returns resource args for each workload, format: [{"cpus": 1.2}, {"cpus": 1.2}]
	// pure calculation
	Alloc(ctx context.Context, nodeName string, deployCount int, resourceOpts coretypes.WorkloadResourceOpts) (*AllocResponse, error)

	// Realloc reallocates resource, returns engine args, delta resource args and final resource args.
	// should return error if resource of some node is not enough for the realloc operation.
	// pure calculation
	Realloc(ctx context.Context, nodeName string, originResourceArgs coretypes.WorkloadResourceArgs, resourceOpts coretypes.WorkloadResourceOpts) (*ReallocResponse, error)
	//Realloc2(ctx context.Context, workloads []*coretypes.Workload, resourceOpts coretypes.RawParams) (*types.//Realloc2Response, error)

	// Remap remaps resources based on workload metadata and node resource usage, then returns engine args for workloads.
	// pure calculation
	Remap(ctx context.Context, nodeName string, workloadMap map[string]*coretypes.Workload) (*RemapResponse, error)

	// UpdateNodeResourceUsage updates node resource usage
	UpdateNodeResourceUsage(ctx context.Context, nodeName string, resourceArgs []coretypes.WorkloadResourceArgs, incr bool) (*UpdateNodeResourceUsageResponse, error)

	// UpdateNodeResourceCapacity updates node resource capacity
	UpdateNodeResourceCapacity(ctx context.Context, nodeName string, resourceOpts coretypes.NodeResourceOpts, incr bool) (*UpdateNodeResourceCapacityResponse, error)

	// AddNode adds a node with requested resource, returns resource capacity and (empty) resource usage
	// should return error if the node already exists
	AddNode(ctx context.Context, nodeName string, resourceOpts coretypes.NodeResourceOpts) (*AddNodeResponse, error)

	// RemoveNode removes node
	RemoveNode(ctx context.Context, nodeName string) (*RemoveNodeResponse, error)

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

// calls the plugin and gets json response
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

	defer log.Infof(ctx, "[callBinaryPlugin] log from plugin %s: %s", bp.path, stderr.String())

	log.Infof(ctx, "[callBinaryPlugin] command: %s %s", bp.path, strings.Join(args, " "))
	if err := command.Run(); err != nil {
		log.Errorf(ctx, "[callBinaryPlugin] failed to run plugin %s, command %v, err %s", bp.path, args, err)
		return err
	}

	stdoutBytes := stdout.Bytes()
	log.Infof(ctx, "[callBinaryPlugin] output from plugin %s: %s", bp.path, string(stdoutBytes))
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
func (bp *BinaryPlugin) GetNodesCapacity(ctx context.Context, nodes []string, resourceOpts coretypes.WorkloadResourceOpts) (resp *GetNodesCapacityResponse, err error) {
	req := GetNodesCapacityRequest{
		NodeNames:    nodes,
		ResourceOpts: resourceOpts,
	}
	resp = &GetNodesCapacityResponse{}
	err = bp.call(ctx, getNodesCapacityCommand, req, resp, bp.timeout)
	return resp, err
}

// GetNodeResourceInfo .
func (bp *BinaryPlugin) GetNodeResourceInfo(ctx context.Context, nodeName string, workloads []*coretypes.Workload, fix bool) (resp *GetNodeResourceInfoResponse, err error) {
	workloadMap := map[string]coretypes.WorkloadResourceArgs{}
	for _, workload := range workloads {
		workloadMap[workload.ID] = workload.ResourceArgs[bp.Name()]
	}

	req := GetNodeResourceInfoRequest{
		NodeName:    nodeName,
		WorkloadMap: workloadMap,
		Fix:         false,
	}
	resp = &GetNodeResourceInfoResponse{}
	if err = bp.call(ctx, getNodeResourceInfoCommand, req, resp, bp.timeout); err != nil {
		return nil, err
	}
	return resp, nil
}

// SetNodeResourceInfo .
func (bp *BinaryPlugin) SetNodeResourceInfo(ctx context.Context, nodeName string, resourceCapacity coretypes.NodeResourceArgs, resourceUsage coretypes.NodeResourceArgs) (*SetNodeResourceInfoResponse, error) {
	req := SetNodeResourceInfoRequest{
		NodeName: nodeName,
		Capacity: resourceCapacity,
		Usage:    resourceUsage,
	}
	resp := &SetNodeResourceInfoResponse{}
	return resp, bp.call(ctx, setNodeResourceInfoCommand, req, resp, bp.timeout)
}

// Alloc .
func (bp *BinaryPlugin) Alloc(ctx context.Context, nodeName string, deployCount int, resourceOpts coretypes.WorkloadResourceOpts) (resp *AllocResponse, err error) {
	req := AllocRequest{
		NodeName:     nodeName,
		DeployCount:  deployCount,
		ResourceOpts: resourceOpts,
	}
	resp = &AllocResponse{}
	if err := bp.call(ctx, allocCommand, req, resp, bp.timeout); err != nil {
		return nil, err
	}
	return resp, nil
}

// Realloc .
func (bp *BinaryPlugin) Realloc(ctx context.Context, nodeName string, originResourceArgs coretypes.WorkloadResourceArgs, resourceOpts coretypes.WorkloadResourceOpts) (resp *ReallocResponse, err error) {
	req := ReallocRequest{
		NodeName:     nodeName,
		Old:          originResourceArgs,
		ResourceOpts: resourceOpts,
	}
	resp = &ReallocResponse{}
	if err := bp.call(ctx, reallocCommand, req, resp, bp.timeout); err != nil {
		return nil, err
	}
	return resp, nil
}

// Remap .
func (bp *BinaryPlugin) Remap(ctx context.Context, nodeName string, workloadMap map[string]*coretypes.Workload) (*RemapResponse, error) {
	workloadResourceArgsMap := map[string]coretypes.WorkloadResourceArgs{}
	for workloadID, workload := range workloadMap {
		workloadResourceArgsMap[workloadID] = workload.ResourceArgs[bp.Name()]
	}

	req := RemapRequest{
		NodeName:    nodeName,
		WorkloadMap: workloadResourceArgsMap,
	}
	resp := &RemapResponse{}
	if err := bp.call(ctx, remapCommand, req, resp, bp.timeout); err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateNodeResourceUsage .
func (bp *BinaryPlugin) UpdateNodeResourceUsage(ctx context.Context, nodeName string, resourceArgs []coretypes.WorkloadResourceArgs, incr bool) (*UpdateNodeResourceUsageResponse, error) {
	req := UpdateNodeResourceUsageRequest{
		NodeName:     nodeName,
		ResourceArgs: resourceArgs,
		Decr:         !incr,
	}
	resp := &UpdateNodeResourceUsageResponse{}
	return resp, bp.call(ctx, updateNodeResourceUsageCommand, req, resp, bp.timeout)
}

// UpdateNodeResourceCapacity ,
func (bp *BinaryPlugin) UpdateNodeResourceCapacity(ctx context.Context, nodeName string, resourceOpts coretypes.NodeResourceOpts, incr bool) (*UpdateNodeResourceCapacityResponse, error) {
	req := UpdateNodeResourceCapacityRequest{
		NodeName:     nodeName,
		ResourceOpts: resourceOpts,
		Decr:         !incr,
	}
	resp := &UpdateNodeResourceCapacityResponse{}
	return resp, bp.call(ctx, updateNodeResourceCapacityCommand, req, resp, bp.timeout)
}

// AddNode .
func (bp *BinaryPlugin) AddNode(ctx context.Context, nodeName string, resourceOpts coretypes.NodeResourceOpts) (resp *AddNodeResponse, err error) {
	req := AddNodeRequest{
		NodeName:     nodeName,
		ResourceOpts: resourceOpts,
	}
	resp = &AddNodeResponse{}
	if err := bp.call(ctx, addNodeCommand, req, resp, bp.timeout); err != nil {
		return nil, err
	}
	return resp, nil
}

// RemoveNode .
func (bp *BinaryPlugin) RemoveNode(ctx context.Context, nodeName string) (*RemoveNodeResponse, error) {
	req := RemoveNodeRequest{
		NodeName: nodeName,
	}
	resp := &RemoveNodeResponse{}
	return resp, bp.call(ctx, removeNodeCommand, req, resp, bp.timeout)
}

// Name .
func (bp *BinaryPlugin) Name() string {
	return bp.path
}
