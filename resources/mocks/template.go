package mocks

import (
	"context"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources"
	"github.com/projecteru2/core/resources/types"
	types2 "github.com/projecteru2/core/types"
	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/mock"
)

func NewMockCpuPlugin() *Plugin {
	m := &Plugin{}
	m.On("LockNodes", mock.Anything, mock.Anything).Return(nil)
	m.On("UnlockNodes", mock.Anything, mock.Anything).Return(nil)
	m.On("GetAvailableNodes", mock.Anything, mock.Anything).Return(map[string]*types.NodeResourceInfo{
		"node1": {
			NodeName: "node1",
			Capacity: 1,
			Usage:    0.5,
			Rate:     0.5,
			Weight:   1,
			Count:    0,
		},
		"node2": {
			NodeName: "node2",
			Capacity: 2,
			Usage:    0.5,
			Rate:     0.5,
			Weight:   1,
			Count:    0,
		},
	}, 6, nil)

	m.On("Alloc", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, deployCount int, rawRequest resources.RawParams) []resources.RawParams {
		log.Infof(ctx, "[Alloc] alloc, node %s, deploy count %v, request %+v", node, deployCount, rawRequest)
		return []resources.RawParams{
			map[string][]string{
				"cpu":  {"1"},
				"file": {"cpu"},
			},
		}
	}, []resources.RawParams{
		map[string][]string{
			"cpu":  {"1"},
			"file": {"cpu"},
		},
	}, nil)

	m.On("Rollback", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, resourceArgs []resources.RawParams) error {
		log.Infof(ctx, "[Rollback] cpu-plugin rollback, node %s, resource args %+v", node, litter.Sdump(resourceArgs))
		return nil
	})

	m.On("Name").Return("cpu-plugin")
	m.On("Remap", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, workloadMap map[string]*types2.Workload) map[string]resources.RawParams {
		log.Infof(ctx, "[Remap] node %v", node)
		res := map[string]resources.RawParams{}
		for workloadID := range workloadMap {
			res[workloadID] = map[string][]string{
				"cpuset-cpus": {"0-65535"}, // I'm rich!
			}
		}
		return res
	}, nil)

	return m
}

func NewMockMemPlugin() *Plugin {
	m := &Plugin{}
	m.On("LockNodes", mock.Anything, mock.Anything).Return(nil)
	m.On("UnlockNodes", mock.Anything, mock.Anything).Return(nil)
	m.On("GetAvailableNodes", mock.Anything, mock.Anything).Return(map[string]*types.NodeResourceInfo{
		"node1": {
			NodeName: "node1",
			Capacity: 1,
			Usage:    0.5,
			Rate:     0.5,
			Weight:   1,
			Count:    0,
		},
		"node2": {
			NodeName: "node2",
			Capacity: 2,
			Usage:    0.5,
			Rate:     0.5,
			Weight:   1,
			Count:    0,
		},
		"node3": {
			NodeName: "node3",
			Capacity: 3,
			Usage:    0.5,
			Rate:     0.5,
			Weight:   1,
			Count:    0,
		},
	}, 6, nil)

	m.On("Alloc", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, deployCount int, rawRequest resources.RawParams) []resources.RawParams {
		log.Infof(ctx, "[Alloc] node %v, deploy count %v, raw request %v", node, deployCount, litter.Sdump(rawRequest))
		return []resources.RawParams{
			map[string][]string{
				"mem":  {"1PB"},
				"file": {"mem"},
			},
		}
	}, []resources.RawParams{
		map[string][]string{
			"mem":  {"1PB"},
			"file": {"mem"},
		},
	}, nil)

	m.On("Rollback", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, resourceArgs []resources.RawParams) error {
		log.Infof(ctx, "[Rollback] mem-plugin rollback, node %s, resource args %+v", node, litter.Sdump(resourceArgs))
		return nil
	})

	m.On("Name").Return("mem-plugin")

	m.On("Remap", mock.Anything, mock.Anything, mock.Anything).Return(map[string]resources.RawParams{}, nil)

	return m
}
