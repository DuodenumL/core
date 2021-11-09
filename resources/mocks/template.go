package mocks

import (
	"context"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources/types"
	coretypes "github.com/projecteru2/core/types"
	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/mock"
)

func NewMockCpuPlugin() *Plugin {
	m := &Plugin{}
	m.On("LockNodes", mock.Anything, mock.Anything).Return(nil)
	m.On("UnlockNodes", mock.Anything, mock.Anything).Return(nil)
	m.On("SelectAvailableNodes", mock.Anything, mock.Anything, mock.Anything).Return(map[string]*types.NodeResourceInfo{
		"node1": {
			NodeName: "node1",
			Capacity: 1,
			Usage:    0.5,
			Rate:     0.5,
			Weight:   1,
		},
		"node2": {
			NodeName: "node2",
			Capacity: 2,
			Usage:    0.5,
			Rate:     0.5,
			Weight:   1,
		},
	}, 6, nil)

	m.On("Alloc", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, deployCount int, rawRequest coretypes.RawParams) []coretypes.RawParams {
		log.Infof(ctx, "[Alloc] alloc, node %s, deploy count %v, request %+v", node, deployCount, rawRequest)
		return []coretypes.RawParams{
			map[string]interface{}{
				"cpu":  1.2,
				"file": []string{"cpu"},
			},
		}
	}, []coretypes.RawParams{
		map[string]interface{}{
			"cpu":  1.2,
			"file": []string{"cpu"},
		},
	}, nil)

	m.On("UpdateNodeResourceUsage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, resourceArgs []coretypes.RawParams, direction bool) error {
		log.Infof(ctx, "[UpdateNodeResourceUsage] cpu-plugin UpdateNodeResourceUsage, incr %v, node %s, resource args %+v", direction, node, litter.Sdump(resourceArgs))
		return nil
	})

	m.On("Name").Return("cpu-plugin")

	m.On("Remap", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, workloadMap map[string]*coretypes.Workload) map[string]coretypes.RawParams {
		log.Infof(ctx, "[Remap] node %v", node)
		res := map[string]coretypes.RawParams{}
		for workloadID := range workloadMap {
			res[workloadID] = map[string]interface{}{
				"cpuset-cpus": []string{"0-65535"}, // I'm rich!
			}
		}
		return res
	}, nil)

	m.On("GetNodeResourceInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		coretypes.RawParams{
			"cpu": "100",
		},
		coretypes.RawParams{
			"cpu": "100",
		},
		[]string{"cpu is sleepy"},
		nil,
	)

	m.On("Realloc", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, originResourceArgs coretypes.RawParams, resourceOpts coretypes.RawParams) coretypes.RawParams {
		log.Infof(ctx, "[Realloc] cpu-plugin realloc workloads, resource opts: %v, resource args: %v", resourceOpts, originResourceArgs)
		return coretypes.RawParams{
			"cpu": 10086,
		}
	}, coretypes.RawParams{
		"cpu": 10086,
	}, coretypes.RawParams{
		"cpu": 10086,
	}, nil)

	m.On("AddNode", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, rawRequest coretypes.RawParams) coretypes.RawParams {
		log.Infof(ctx, "cpu-plugin add node %v, req: %+v", node, rawRequest)
		return coretypes.RawParams{
			"cpu": 65535,
		}
	}, coretypes.RawParams{
		"cpu": 0,
	}, nil)

	m.On("RemoveNode", mock.Anything, mock.Anything).Return(func(ctx context.Context, node string) error {
		log.Infof(ctx, "cpu-plugin remove node %v", node)
		return nil
	})

	return m
}

func NewMockMemPlugin() *Plugin {
	m := &Plugin{}
	m.On("LockNodes", mock.Anything, mock.Anything).Return(nil)
	m.On("UnlockNodes", mock.Anything, mock.Anything).Return(nil)
	m.On("SelectAvailableNodes", mock.Anything, mock.Anything, mock.Anything).Return(map[string]*types.NodeResourceInfo{
		"node1": {
			NodeName: "node1",
			Capacity: 1,
			Usage:    0.5,
			Rate:     0.5,
			Weight:   1,
		},
		"node2": {
			NodeName: "node2",
			Capacity: 2,
			Usage:    0.5,
			Rate:     0.5,
			Weight:   1,
		},
		"node3": {
			NodeName: "node3",
			Capacity: 3,
			Usage:    0.5,
			Rate:     0.5,
			Weight:   1,
		},
	}, 6, nil)

	m.On("Alloc", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, deployCount int, rawRequest coretypes.RawParams) []coretypes.RawParams {
		log.Infof(ctx, "[Alloc] node %v, deploy count %v, raw request %v", node, deployCount, litter.Sdump(rawRequest))
		return []coretypes.RawParams{
			map[string]interface{}{
				"mem":  "1PB",
				"file": []string{"mem"},
			},
		}
	}, []coretypes.RawParams{
		map[string]interface{}{
			"mem":  "1PB",
			"file": []string{"mem"},
		},
	}, nil)

	m.On("UpdateNodeResourceUsage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, resourceArgs []coretypes.RawParams, direction bool) error {
		log.Infof(ctx, "[UpdateNodeResourceUsage] mem-plugin UpdateNodeResourceUsage, incr %v, node %s, resource args %+v", direction, node, litter.Sdump(resourceArgs))
		return nil
	})

	m.On("Name").Return("mem-plugin")

	m.On("Remap", mock.Anything, mock.Anything, mock.Anything).Return(map[string]coretypes.RawParams{}, nil)

	m.On("GetNodeResourceInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		coretypes.RawParams{
			"mem_cap": "10000PB",
		},
		coretypes.RawParams{
			"mem_cap": "10000PB",
		},
		[]string{"the mem_cap doesn't look like a machine on earth"},
		nil,
	)

	m.On("Realloc", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, originResourceArgs coretypes.RawParams, resourceOpts coretypes.RawParams) coretypes.RawParams {
		log.Infof(ctx, "[Realloc] mem-plugin realloc workloads, resource opts: %v, resource args: %v", resourceOpts, originResourceArgs)
		return coretypes.RawParams{
			"mem": "1000000PB",
		}
	}, coretypes.RawParams{
		"mem": "1000000PB",
	}, coretypes.RawParams{
		"mem": "1000000PB",
	}, nil)

	m.On("AddNode", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, rawRequest coretypes.RawParams) coretypes.RawParams {
		log.Infof(ctx, "mem-plugin add node %v, req: %+v", node, rawRequest)
		return coretypes.RawParams{
			"mem": 65535,
		}
	}, coretypes.RawParams{
		"mem": 0,
	}, nil)

	m.On("RemoveNode", mock.Anything, mock.Anything).Return(func(ctx context.Context, node string) error {
		log.Infof(ctx, "mem-plugin remove node %v", node)
		return nil
	})

	return m
}
