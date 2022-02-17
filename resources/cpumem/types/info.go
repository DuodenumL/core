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
