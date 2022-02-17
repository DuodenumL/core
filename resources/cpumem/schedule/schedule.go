package schedule

import (
	"context"
	"math"
	"sort"

	"github.com/pkg/errors"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources"
	"github.com/projecteru2/core/resources/cpumem/types"
	"github.com/projecteru2/core/utils"
)

// Schedule .
func Schedule(ctx context.Context, nodes []*types.NodeResourceInfo, nodeNames []string, request *types.WorkloadResourceOpts, maxShare, shareBase int) (capacityMap map[string]int, cpuPlans map[string][]types.CPUMap, total int, err error) {
	scheduleInfos := []types.ScheduleInfo{}
	for i, node := range nodes {
		scheduleInfos = append(scheduleInfos, types.NodeResourceInfoToScheduleInfo(node, nodeNames[i]))
	}

	if !request.CPUBind || request.CPURequest == 0 {
		scheduleInfos, total, err = SelectMemoryNodes(ctx, scheduleInfos, request.CPURequest, request.MemRequest)
	} else {
		scheduleInfos, cpuPlans, total, err = SelectCPUNodes(ctx, scheduleInfos, request.CPURequest, request.MemRequest, maxShare, shareBase)
	}

	capacityMap = map[string]int{}
	for _, info := range scheduleInfos {
		capacityMap[info.Name] = info.Capacity
	}

	return capacityMap, cpuPlans, total, err
}

// SelectMemoryNodes filter nodes with enough memory
func SelectMemoryNodes(ctx context.Context, scheduleInfos []types.ScheduleInfo, quota float64, memory int64) ([]types.ScheduleInfo, int, error) {
	resources := []struct {
		Nodename string
		CPU      types.CPUMap
		Memory   int64
	}{}
	for _, scheduleInfo := range scheduleInfos {
		resources = append(resources, struct {
			Nodename string
			CPU      types.CPUMap
			Memory   int64
		}{scheduleInfo.Name, scheduleInfo.CPU, scheduleInfo.MemCap})
	}
	log.Infof(ctx, "[SelectMemoryNodes] resources: %v, need cpu: %f, memory: %d", resources, quota, memory)
	scheduleInfosLength := len(scheduleInfos)

	// 筛选出能满足 CPU 需求的
	sort.Slice(scheduleInfos, func(i, j int) bool { return len(scheduleInfos[i].CPU) < len(scheduleInfos[j].CPU) })
	p := sort.Search(scheduleInfosLength, func(i int) bool {
		return float64(len(scheduleInfos[i].CPU)) >= quota
	})
	// p 最大也就是 scheduleInfosLength - 1
	if p == scheduleInfosLength {
		return nil, 0, errors.Wrapf(types.ErrInsufficientCPU, "no node remains cpu more than %0.2f", quota)
	}
	scheduleInfosLength -= p
	scheduleInfos = scheduleInfos[p:]

	// 计算是否有足够的内存满足需求
	sort.Slice(scheduleInfos, func(i, j int) bool { return scheduleInfos[i].MemCap < scheduleInfos[j].MemCap })
	p = sort.Search(scheduleInfosLength, func(i int) bool { return scheduleInfos[i].MemCap >= memory })
	if p == scheduleInfosLength {
		return nil, 0, errors.Wrapf(types.ErrInsufficientMem, "no node remains memory more than %d bytes", memory)
	}
	scheduleInfos = scheduleInfos[p:]

	// 这里 memCap 一定是大于 memory 的所以不用判断 cap 内容
	volTotal := 0
	for i, scheduleInfo := range scheduleInfos {
		capacity := math.MaxInt32
		if memory != 0 {
			capacity = int(scheduleInfo.MemCap / memory)
		}
		volTotal += capacity
		scheduleInfos[i].Capacity = capacity
	}
	return scheduleInfos, volTotal, nil
}

// SelectCPUNodes select nodes with enough cpus
func SelectCPUNodes(ctx context.Context, scheduleInfos []types.ScheduleInfo, quota float64, memory int64, maxShare, shareBase int) ([]types.ScheduleInfo, map[string][]types.CPUMap, int, error) {
	resources := []struct {
		Nodename string
		Memory   int64
		CPU      types.CPUMap
	}{}
	for _, scheduleInfo := range scheduleInfos {
		resources = append(resources, struct {
			Nodename string
			Memory   int64
			CPU      types.CPUMap
		}{scheduleInfo.Name, scheduleInfo.MemCap, scheduleInfo.CPU})
	}
	log.Infof(ctx, "[SelectCPUNodes] resources %v, need cpu: %f memory: %d", resources, quota, memory)
	if quota <= 0 {
		return nil, nil, 0, errors.WithStack(types.ErrInvalidCPU)
	}
	if len(scheduleInfos) == 0 {
		return nil, nil, 0, errors.WithStack(types.ErrNoNode)
	}

	return cpuPriorPlan(ctx, quota, memory, scheduleInfos, maxShare, shareBase)
}

// ReselectCPUNodes used for realloc one container with cpu affinity
func ReselectCPUNodes(ctx context.Context, nodeResourceInfo *types.NodeResourceInfo, nodeName string, CPU types.CPUMap, quota float64, memory int64, maxShare, shareBase int) (map[string][]types.CPUMap, int, error) {
	scheduleInfo := types.NodeResourceInfoToScheduleInfo(nodeResourceInfo, nodeName)
	log.Infof(ctx, "[ReselectCPUNodes] resources %v, need cpu %f, need memory %d, existing %v",
		struct {
			Nodename string
			Memory   int64
			CPU      types.CPUMap
		}{scheduleInfo.Name, scheduleInfo.MemCap, scheduleInfo.CPU},
		quota, memory, CPU)
	var affinityPlan types.CPUMap
	// remaining quota that's impossible to achieve affinity
	if scheduleInfo, quota, affinityPlan = cpuReallocPlan(scheduleInfo, quota, CPU, int64(shareBase)); quota == 0 {
		cpuPlans := map[string][]types.CPUMap{
			scheduleInfo.Name: {
				affinityPlan,
			},
		}
		scheduleInfo.Capacity = 1
		return cpuPlans, 1, nil
	}

	_, cpuPlans, total, err := SelectCPUNodes(ctx, []types.ScheduleInfo{scheduleInfo}, quota, memory, maxShare, shareBase)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to reschedule cpu")
	}

	// add affinity plans
	for i, plan := range cpuPlans[scheduleInfo.Name] {
		for cpuID, pieces := range affinityPlan {
			if _, ok := plan[cpuID]; ok {
				cpuPlans[scheduleInfo.Name][i][cpuID] += pieces
			} else {
				cpuPlans[scheduleInfo.Name][i][cpuID] = pieces
			}
		}
	}
	return cpuPlans, total, nil
}

func cpuPriorPlan(ctx context.Context, cpu float64, memory int64, scheduleInfos []types.ScheduleInfo, maxShareCore, coreShare int) ([]types.ScheduleInfo, map[string][]types.CPUMap, int, error) {
	var nodeWorkload = map[string][]types.CPUMap{}
	volTotal := 0

	for p, scheduleInfo := range scheduleInfos {
		// 统计全局 CPU，为非 numa 或者跨 numa 计算
		globalCPUMap := scheduleInfo.CPU
		// 统计全局 Memory
		globalMemCap := scheduleInfo.MemCap
		// 计算每个 numa node 的分配策略
		// 得到 numa CPU 分组
		numaCPUMap := map[string]types.CPUMap{}
		for cpuID, nodeID := range scheduleInfo.NUMA {
			if _, ok := numaCPUMap[nodeID]; !ok {
				numaCPUMap[nodeID] = types.CPUMap{}
			}
			cpuCount, ok := scheduleInfo.CPU[cpuID]
			if !ok {
				continue
			}
			numaCPUMap[nodeID][cpuID] = cpuCount
		}
		for nodeID, nodeCPUMap := range numaCPUMap {
			nodeMemCap, ok := scheduleInfo.NUMAMemory[nodeID]
			if !ok {
				continue
			}
			cap, plan := calculateCPUPlan(nodeCPUMap, nodeMemCap, cpu, memory, maxShareCore, coreShare)
			if cap > 0 {
				if _, ok := nodeWorkload[scheduleInfo.Name]; !ok {
					nodeWorkload[scheduleInfo.Name] = []types.CPUMap{}
				}
				volTotal += cap
				globalMemCap -= int64(cap) * memory
				for _, cpuPlan := range plan {
					globalCPUMap.Sub(cpuPlan)
					nodeWorkload[scheduleInfo.Name] = append(nodeWorkload[scheduleInfo.Name], cpuPlan)
				}
			}
			log.Infof(ctx, "[cpuPriorPlan] node %s numa node %s deploy capacity %d", scheduleInfo.Name, nodeID, cap)
		}
		// 非 numa
		// 或者是扣掉 numa 分配后剩下的资源里面
		cap, plan := calculateCPUPlan(globalCPUMap, globalMemCap, cpu, memory, maxShareCore, coreShare)
		if cap > 0 {
			if _, ok := nodeWorkload[scheduleInfo.Name]; !ok {
				nodeWorkload[scheduleInfo.Name] = []types.CPUMap{}
			}
			scheduleInfos[p].Capacity += cap
			volTotal += cap
			nodeWorkload[scheduleInfo.Name] = append(nodeWorkload[scheduleInfo.Name], plan...)
		}
		log.Infof(ctx, "[cpuPriorPlan] node %s total deploy capacity %d", scheduleInfo.Name, scheduleInfos[p].Capacity)
	}

	// 裁剪掉不能部署的
	sort.Slice(scheduleInfos, func(i, j int) bool { return scheduleInfos[i].Capacity < scheduleInfos[j].Capacity })
	p := sort.Search(len(scheduleInfos), func(i int) bool { return scheduleInfos[i].Capacity > 0 })
	if p == len(scheduleInfos) {
		return nil, nil, 0, errors.Wrapf(types.ErrInsufficientResource, "no node remains %.2f pieces of cpu and %d bytes of memory at the same time", cpu, memory)
	}

	return scheduleInfos[p:], nodeWorkload, volTotal, nil
}

func calculateCPUPlan(CPUMap types.CPUMap, MemCap int64, cpu float64, memory int64, maxShareCore, coreShare int) (int, []types.CPUMap) {
	host := resources.NewHost(resources.ResourceMap(CPUMap), coreShare)
	resourceMaps := host.DistributeOneRation(cpu, maxShareCore)
	plan := []types.CPUMap{}
	for _, resourceMap := range resourceMaps {
		plan = append(plan, types.CPUMap(resourceMap))
	}
	memLimit := math.MaxInt64
	if memory != 0 {
		memLimit = utils.Max(int(MemCap/memory), 0)
	}
	cap := len(plan) // 每个node可以放的容器数
	if cap > memLimit {
		plan = plan[:memLimit]
		cap = memLimit
	}
	if cap <= 0 {
		plan = nil
	}
	return cap, plan
}

func cpuReallocPlan(scheduleInfo types.ScheduleInfo, quota float64, CPU types.CPUMap, sharebase int64) (types.ScheduleInfo, float64, types.CPUMap) {
	affinityPlan := make(types.CPUMap)
	diff := int64(quota*float64(sharebase)) - CPU.TotalPieces()
	// sort by pieces
	cpuIDs := []string{}
	for cpuID := range CPU {
		cpuIDs = append(cpuIDs, cpuID)
	}
	sort.Slice(cpuIDs, func(i, j int) bool { return CPU[cpuIDs[i]] < CPU[cpuIDs[j]] })

	// shrink, ensure affinity
	if diff <= 0 {
		affinityPlan = CPU
		// prioritize fragments
		for _, cpuID := range cpuIDs {
			if diff == 0 {
				break
			}
			shrink := utils.Min64(affinityPlan[cpuID], -diff)
			affinityPlan[cpuID] -= shrink
			if affinityPlan[cpuID] == 0 {
				delete(affinityPlan, cpuID)
			}
			diff += shrink
			scheduleInfo.CPU[cpuID] += shrink
		}
		return scheduleInfo, float64(0), affinityPlan
	}

	// expand, prioritize full cpus
	needPieces := int64(quota * float64(sharebase))
	for i := len(cpuIDs) - 1; i >= 0; i-- {
		cpuID := cpuIDs[i]
		if needPieces == 0 {
			scheduleInfo.CPU[cpuID] += CPU[cpuID]
			continue
		}

		// whole cpu, keep it
		if CPU[cpuID] == sharebase {
			affinityPlan[cpuID] = sharebase
			needPieces -= sharebase
			continue
		}

		// fragments, try to find complement
		if available := scheduleInfo.CPU[cpuID]; available == sharebase-CPU[cpuID] {
			expand := utils.Min64(available, needPieces)
			affinityPlan[cpuID] = CPU[cpuID] + expand
			scheduleInfo.CPU[cpuID] -= expand
			needPieces -= sharebase
			continue
		}

		// else, return to cpu pools
		scheduleInfo.CPU[cpuID] += CPU[cpuID]
	}

	return scheduleInfo, float64(needPieces) / float64(sharebase), affinityPlan
}
