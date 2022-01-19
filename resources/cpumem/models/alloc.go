package models

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/projecteru2/core/resources/cpumem/schedule"
	"github.com/projecteru2/core/resources/cpumem/types"
)

// GetDeployArgs .
func (c *CPUMem) GetDeployArgs(ctx context.Context, node string, deployCount int, opts *types.WorkloadResourceOpts) ([]*types.EngineArgs, []*types.WorkloadResourceArgs, error) {
	if err := opts.Validate(); err != nil {
		logrus.Errorf("[GetDeployArgs] invalid resource opts %+v, err: %v", opts, err)
		return nil, nil, err
	}

	resourceInfo, err := c.doGetNodeResourceInfo(ctx, node)
	if err != nil {
		logrus.Errorf("[GetDeployArgs] failed to get resource info of node %v, err: %v", node, err)
		return nil, nil, err
	}

	if !opts.CPUBind {
		return c.doAllocByMemory(resourceInfo, deployCount, opts)
	}

	return c.doAllocByCPU(ctx, resourceInfo, deployCount, opts)
}

func (c *CPUMem) doAllocByMemory(resourceInfo *types.NodeResourceInfo, deployCount int, opts *types.WorkloadResourceOpts) ([]*types.EngineArgs, []*types.WorkloadResourceArgs, error) {
	if opts.CPURequest > float64(len(resourceInfo.Capacity.CPUMap)) {
		return nil, nil, types.ErrInsufficientCPU
	}

	availableResourceArgs := resourceInfo.GetAvailableResource()
	if opts.MemRequest > 0 && availableResourceArgs.Memory/opts.MemRequest < int64(deployCount) {
		return nil, nil, types.ErrInsufficientMem
	}

	resEngineArgs := []*types.EngineArgs{}
	resResourceArgs := []*types.WorkloadResourceArgs{}

	engineArgs := &types.EngineArgs{
		CPU:    opts.CPULimit,
		Memory: opts.MemLimit,
	}
	resourceArgs := &types.WorkloadResourceArgs{
		CPURequest:    opts.CPURequest,
		CPULimit:      opts.CPULimit,
		MemoryRequest: opts.MemRequest,
		MemoryLimit:   opts.MemLimit,
	}

	for len(resEngineArgs) < deployCount {
		resEngineArgs = append(resEngineArgs, engineArgs)
		resResourceArgs = append(resResourceArgs, resourceArgs)
	}
	return resEngineArgs, resResourceArgs, nil
}

func (c *CPUMem) doAllocByCPU(ctx context.Context, resourceInfo *types.NodeResourceInfo, deployCount int, opts *types.WorkloadResourceOpts) ([]*types.EngineArgs, []*types.WorkloadResourceArgs, error) {
	_, cpuPlanMap, total, err := schedule.Schedule(ctx, []*types.NodeResourceInfo{resourceInfo}, []string{""}, opts, c.config.Scheduler.MaxShare, c.config.Scheduler.ShareBase)
	if err != nil {
		logrus.Errorf("[doAllocByCPU] failed to schedule, err: %v", err)
		return nil, nil, err
	}

	if total < deployCount {
		return nil, nil, types.ErrInsufficientResource
	}

	cpuPlans := []types.CPUMap{}
	for _, plans := range cpuPlanMap {
		cpuPlans = append(cpuPlans, plans...)
	}

	cpuPlans = cpuPlans[:deployCount]
	resEngineArgs := []*types.EngineArgs{}
	resResourceArgs := []*types.WorkloadResourceArgs{}

	for _, cpuMap := range cpuPlans {
		resEngineArgs = append(resEngineArgs, &types.EngineArgs{
			CPU:      opts.CPULimit,
			CPUMap:   cpuMap,
			NUMANode: c.getNUMANodeID(cpuMap, resourceInfo.Capacity.NUMA),
			Memory:   opts.MemLimit,
		})

		resourceArgs := &types.WorkloadResourceArgs{
			CPURequest:    opts.CPURequest,
			CPULimit:      opts.CPULimit,
			MemoryRequest: opts.MemRequest,
			MemoryLimit:   opts.MemLimit,
			CPUMap:        cpuMap,
			NUMANode:      c.getNUMANodeID(cpuMap, resourceInfo.Capacity.NUMA),
		}
		// may cause bugs in future
		// because the issue of getNUMANodeID
		// however it's good in the binary version (core-plugins)
		if len(resourceArgs.NUMANode) > 0 {
			resourceArgs.NUMAMemory = types.NUMAMemory{resourceArgs.NUMANode: resourceArgs.MemoryRequest}
		}

		resResourceArgs = append(resResourceArgs, resourceArgs)
	}

	return resEngineArgs, resResourceArgs, nil
}

// getNUMANodeID returns the NUMA node ID of the given CPU map.
func (c *CPUMem) getNUMANodeID(cpuMap types.CPUMap, numa types.NUMA) string {
	nodeID := ""
	for cpuID := range cpuMap {
		if memoryNode, ok := numa[cpuID]; ok {
			if nodeID == "" {
				nodeID = memoryNode
			} else if nodeID != memoryNode { // 如果跨 NODE 了，让系统决定 nodeID
				nodeID = ""
			}
		}
	}
	return nodeID
}
