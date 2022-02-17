package types

// ScheduleInfo for scheduler
// to compatible with old version
type ScheduleInfo struct {
	Name string

	StorageCap     int64     `json:"storage_cap"`
	InitStorageCap int64     `json:"init_storage_cap"`
	Volume         VolumeMap `json:"volume"`
	InitVolume     VolumeMap `json:"init_volume"`

	VolumePlans []VolumePlan // {{"AUTO:/data:rw:1024": "/mnt0:/data:rw:1024"}}
	Capacity    int          // 可以部署几个
}

// NodeResourceInfoToScheduleInfo converts NodeResourceInfo to ScheduleInfo
func NodeResourceInfoToScheduleInfo(info *NodeResourceInfo, nodeName string) ScheduleInfo {
	scheduleInfo := ScheduleInfo{
		Name:           nodeName,
		StorageCap:     info.Capacity.Storage - info.Usage.Storage,
		InitStorageCap: info.Capacity.Storage,
		Volume:         info.Capacity.Volumes,
		InitVolume:     info.Capacity.Volumes,
	}
	scheduleInfo.Volume.Sub(info.Usage.Volumes)

	return scheduleInfo
}
