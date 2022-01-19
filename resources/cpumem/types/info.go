package types

// ScheduleInfo for scheduler
// to compatible with old version
type ScheduleInfo struct {
	Name string

	CPU            CPUMap
	NUMA           NUMA
	NUMAMemory     NUMAMemory
	MemCap         int64
	InitCPU        CPUMap
	InitMemCap     int64
	InitNUMAMemory NUMAMemory

	CPUPlan  []CPUMap
	Capacity int // 可以部署几个
}

// ScheduleInfoToNodeResourceInfo converts ScheduleInfo to NodeResourceInfo
func ScheduleInfoToNodeResourceInfo(info ScheduleInfo) *NodeResourceInfo {
	return &NodeResourceInfo{
		Capacity: &NodeResourceArgs{
			CPU:        float64(len(info.InitCPU)),
			CPUMap:     info.InitCPU,
			Memory:     info.InitMemCap,
			NUMAMemory: info.InitNUMAMemory,
			NUMA:       info.NUMA,
		},
		Usage: &NodeResourceArgs{
			CPU:        float64(len(info.CPU)),
			CPUMap:     info.CPU,
			Memory:     info.MemCap,
			NUMAMemory: info.NUMAMemory,
			NUMA:       nil,
		},
	}
}

// NodeResourceInfoToScheduleInfo converts NodeResourceInfo to ScheduleInfo
func NodeResourceInfoToScheduleInfo(info *NodeResourceInfo, nodeName string) ScheduleInfo {
	scheduleInfo := ScheduleInfo{
		Name:           nodeName,
		CPU:            info.Capacity.CPUMap,
		NUMA:           info.Capacity.NUMA,
		NUMAMemory:     info.Capacity.NUMAMemory,
		MemCap:         info.Capacity.Memory,
		InitCPU:        info.Capacity.CPUMap,
		InitMemCap:     info.Capacity.Memory,
		InitNUMAMemory: info.Capacity.NUMAMemory,
	}
	scheduleInfo.CPU.Sub(info.Usage.CPUMap)
	scheduleInfo.NUMAMemory.Sub(info.Usage.NUMAMemory)
	scheduleInfo.MemCap -= info.Usage.Memory

	return scheduleInfo
}
