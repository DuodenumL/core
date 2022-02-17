package schedule

import (
	"context"
	"math"
	"sort"

	"github.com/pkg/errors"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources"
	volumetypes "github.com/projecteru2/core/resources/volume/types"
	"github.com/projecteru2/core/utils"
)

// Schedule .
func Schedule(ctx context.Context, nodes []*volumetypes.NodeResourceInfo, nodeNames []string, request *volumetypes.WorkloadResourceOpts) (capacityMap map[string]int, volumePlans map[string][]volumetypes.VolumePlan, total int, err error) {
	scheduleInfos := []volumetypes.ScheduleInfo{}
	for i, node := range nodes {
		scheduleInfos = append(scheduleInfos, volumetypes.NodeResourceInfoToScheduleInfo(node, nodeNames[i]))
	}

	scheduleInfos, total, err = SelectStorageNodes(ctx, scheduleInfos, request.StorageRequest)
	if err != nil {
		return nil, nil, 0, err
	}
	scheduleInfos, volumePlans, total, err = SelectVolumeNodes(ctx, scheduleInfos, request.VolumesRequest)
	if err != nil {
		return nil, nil, 0, err
	}

	capacityMap = map[string]int{}
	for _, scheduleInfo := range scheduleInfos {
		capacityMap[scheduleInfo.Name] = scheduleInfo.Capacity
	}
	return capacityMap, volumePlans, total, nil
}

// SelectStorageNodes filters nodes with enough storage
func SelectStorageNodes(ctx context.Context, scheduleInfos []volumetypes.ScheduleInfo, storage int64) ([]volumetypes.ScheduleInfo, int, error) {
	switch {
	case storage < 0:
		return nil, 0, errors.WithStack(volumetypes.ErrInvalidStorage)
	case storage == 0:
		for i := range scheduleInfos {
			scheduleInfos[i].Capacity = math.MaxInt64
		}
		return scheduleInfos, math.MaxInt64, nil
	default:
		storages := []struct {
			Nodename string
			Storage  int64
		}{}
		for _, scheduleInfo := range scheduleInfos {
			storages = append(storages, struct {
				Nodename string
				Storage  int64
			}{scheduleInfo.Name, scheduleInfo.StorageCap})
		}
		log.Infof(ctx, "[SelectStorageNodes] resources: %v, need: %d", storages, storage)
	}

	leng := len(scheduleInfos)

	sort.Slice(scheduleInfos, func(i, j int) bool { return scheduleInfos[i].StorageCap < scheduleInfos[j].StorageCap })
	p := sort.Search(leng, func(i int) bool { return scheduleInfos[i].StorageCap >= storage })
	if p == leng {
		return nil, 0, errors.Wrapf(volumetypes.ErrInsufficientResource, "no node remains storage more than %d bytes", storage)
	}

	scheduleInfos = scheduleInfos[p:]

	total := 0
	for i := range scheduleInfos {
		storCap := int(scheduleInfos[i].StorageCap / storage)
		total += updateScheduleInfoCapacity(&scheduleInfos[i], storCap)
	}

	return scheduleInfos, total, nil
}

// SelectVolumeNodes calculates plans for volume request
func SelectVolumeNodes(ctx context.Context, scheduleInfos []volumetypes.ScheduleInfo, vbs volumetypes.VolumeBindings) ([]volumetypes.ScheduleInfo, map[string][]volumetypes.VolumePlan, int, error) {
	resources := []struct {
		Nodename string
		Volume   volumetypes.VolumeMap
	}{}
	for _, scheduleInfo := range scheduleInfos {
		resources = append(resources, struct {
			Nodename string
			Volume   volumetypes.VolumeMap
		}{scheduleInfo.Name, scheduleInfo.Volume})
	}
	log.Infof(ctx, "[SelectVolumeNodes] resources %v, need volume: %v", resources, vbs.ToStringSlice(true, true))

	var reqsNorm, reqsMono []int64
	var vbsNorm, vbsMono, vbsUnlimited volumetypes.VolumeBindings
	volTotal := 0

	for _, vb := range vbs {
		switch {
		case vb.RequireScheduleMonopoly():
			vbsMono = append(vbsMono, vb)
			reqsMono = append(reqsMono, vb.SizeInBytes)
		case vb.RequireScheduleUnlimitedQuota():
			vbsUnlimited = append(vbsUnlimited, vb)
		case vb.RequireSchedule():
			vbsNorm = append(vbsNorm, vb)
			reqsNorm = append(reqsNorm, vb.SizeInBytes)
		}
	}

	if len(vbsNorm) == 0 && len(vbsMono) == 0 && len(vbsUnlimited) == 0 {
		for i := range scheduleInfos {
			capacity := updateScheduleInfoCapacity(&scheduleInfos[i], math.MaxInt64)
			if volTotal == math.MaxInt64 || capacity == math.MaxInt64 {
				volTotal = math.MaxInt64
			} else {
				volTotal += capacity
			}
		}
		return scheduleInfos, nil, volTotal, nil
	}

	volumePlans := map[string][]volumetypes.VolumePlan{}
	for idx, scheduleInfo := range scheduleInfos {
		if len(scheduleInfo.Volume) == 0 {
			volTotal += scheduleInfo.Capacity
			continue
		}

		usedVolumeMap, unusedVolumeMap := scheduleInfo.Volume.SplitByUsed(scheduleInfo.InitVolume)
		if len(reqsMono) == 0 {
			usedVolumeMap.Add(unusedVolumeMap)
		}
		if len(reqsNorm) != 0 && len(usedVolumeMap) == 0 && len(unusedVolumeMap) != 0 {
			usedVolumeMap = volumetypes.VolumeMap{}
			// give out half of volumes
			half, cnt, toDelete := (len(unusedVolumeMap)+1)/2, 0, []string{}
			for i, v := range unusedVolumeMap {
				cnt++
				if cnt > half {
					break
				}
				toDelete = append(toDelete, i)
				usedVolumeMap[i] = v
			}
			for _, i := range toDelete {
				delete(unusedVolumeMap, i)
			}
		}

		capNorm, plansNorm := calculateVolumePlan(usedVolumeMap, reqsNorm)
		capMono, plansMono := calculateMonopolyVolumePlan(scheduleInfo.InitVolume, unusedVolumeMap, reqsMono)

		volTotal += updateScheduleInfoCapacity(&scheduleInfos[idx], utils.Min(capNorm, capMono))
		cap := scheduleInfos[idx].Capacity

		volumePlans[scheduleInfo.Name] = make([]volumetypes.VolumePlan, cap)
		for idx := range volumePlans[scheduleInfo.Name] {
			volumePlans[scheduleInfo.Name][idx] = volumetypes.VolumePlan{}
		}
		if plansNorm != nil {
			for i, plan := range plansNorm[:cap] {
				volumePlans[scheduleInfo.Name][i].Merge(volumetypes.MakeVolumePlan(vbsNorm, plan))
			}
		}
		if plansMono != nil {
			for i, plan := range plansMono[:cap] {
				volumePlans[scheduleInfo.Name][i].Merge(volumetypes.MakeVolumePlan(vbsMono, plan))

			}
		}

		if len(vbsUnlimited) > 0 {
			// select the device with the most capacity as unlimited plan volume
			volume := volumetypes.VolumeMap{"": 0}
			currentMaxAvailable := int64(0)
			for vol, available := range scheduleInfo.Volume {
				if available > currentMaxAvailable {
					currentMaxAvailable = available
					volume = volumetypes.VolumeMap{vol: 0}
				}
			}

			planUnlimited := volumetypes.VolumePlan{}
			for _, vb := range vbsUnlimited {
				planUnlimited[*vb] = volume
			}

			for i := range volumePlans[scheduleInfo.Name] {
				volumePlans[scheduleInfo.Name][i].Merge(planUnlimited)
			}
		}
	}

	sort.Slice(scheduleInfos, func(i, j int) bool { return scheduleInfos[i].Capacity < scheduleInfos[j].Capacity })
	p := sort.Search(len(scheduleInfos), func(i int) bool { return scheduleInfos[i].Capacity > 0 })
	if p == len(scheduleInfos) {
		return nil, nil, 0, errors.Wrapf(volumetypes.ErrInsufficientResource, "no node remains volumes for requests %+v", vbs.ToStringSlice(true, true))
	}

	return scheduleInfos[p:], volumePlans, volTotal, nil
}

// ReselectVolumeNodes is used for realloc only
func ReselectVolumeNodes(ctx context.Context, node *volumetypes.NodeResourceInfo, existing volumetypes.VolumePlan, vbsReq volumetypes.VolumeBindings) (map[string][]volumetypes.VolumePlan, int, error) {
	scheduleInfo := volumetypes.NodeResourceInfoToScheduleInfo(node, "")

	log.Infof(ctx, "[ReselectVolumeNodes] resources: %v, need volume: %v, existing %v",
		struct {
			Nodename   string
			Volume     volumetypes.VolumeMap
			InitVolume volumetypes.VolumeMap
		}{scheduleInfo.Name, scheduleInfo.Volume, scheduleInfo.InitVolume},
		vbsReq.ToStringSlice(true, true), existing.ToLiteral())
	affinityPlan := volumetypes.VolumePlan{}
	needReschedule := volumetypes.VolumeBindings{}
	norm, mono, unlim := distinguishVolumeBindings(vbsReq)

	// norm
	normAff, normRem := distinguishAffinityVolumeBindings(norm, existing)
	needReschedule = append(needReschedule, normRem...)
	for _, vb := range normAff {
		_, oldVM, _ := existing.FindAffinityPlan(*vb)
		if scheduleInfo.Volume[oldVM.GetResourceID()] < vb.SizeInBytes {
			return nil, 0, errors.Wrapf(volumetypes.ErrInsufficientResource, "no space to expand: %+v, %+v", oldVM, vb)
		}
		affinityPlan.Merge(volumetypes.VolumePlan{
			*vb: volumetypes.VolumeMap{oldVM.GetResourceID(): vb.SizeInBytes},
		})
		scheduleInfo.Volume[oldVM.GetResourceID()] -= vb.SizeInBytes
	}

	// mono
	monoAff, _ := distinguishAffinityVolumeBindings(mono, existing)
	if len(monoAff) == 0 {
		// all reschedule
		needReschedule = append(needReschedule, mono...)

	} else {
		// all no reschedule
		_, oldVM, _ := existing.FindAffinityPlan(*monoAff[0])
		monoVolume := oldVM.GetResourceID()
		newVms, monoTotal, newMonoPlan := []volumetypes.VolumeMap{}, int64(0), volumetypes.VolumePlan{}
		for _, vb := range mono {
			monoTotal += vb.SizeInBytes
			newVms = append(newVms, volumetypes.VolumeMap{monoVolume: vb.SizeInBytes})
		}
		if monoTotal > scheduleInfo.InitVolume[monoVolume] {
			return nil, 0, errors.Wrap(volumetypes.ErrInsufficientResource, "no space to expand mono volumes: ")
		}
		newVms = proportionPlan(newVms, scheduleInfo.InitVolume[monoVolume])
		for i, vb := range mono {
			newMonoPlan[*vb] = newVms[i]
		}
		affinityPlan.Merge(newMonoPlan)
		scheduleInfo.Volume[monoVolume] = 0
	}

	// unlimit
	unlimAff, unlimRem := distinguishAffinityVolumeBindings(unlim, existing)
	needReschedule = append(needReschedule, unlimRem...)
	unlimPlan := volumetypes.VolumePlan{}
	for _, vb := range unlimAff {
		_, oldVM, _ := existing.FindAffinityPlan(*vb)
		unlimPlan[*vb] = oldVM
	}
	affinityPlan.Merge(unlimPlan)

	// schedule new volume requests
	if len(needReschedule) == 0 {
		scheduleInfo.Capacity = 1
		return map[string][]volumetypes.VolumePlan{scheduleInfo.Name: {affinityPlan}}, 1, nil
	}
	_, volumePlans, total, err := SelectVolumeNodes(ctx, []volumetypes.ScheduleInfo{scheduleInfo}, needReschedule)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to reschedule volume")
	}

	// merge
	for i := range volumePlans[scheduleInfo.Name] {
		volumePlans[scheduleInfo.Name][i].Merge(affinityPlan)
	}

	return volumePlans, total, nil
}

func calculateVolumePlan(volumeMap volumetypes.VolumeMap, required []int64) (int, [][]volumetypes.VolumeMap) {
	if len(required) == 0 {
		return math.MaxInt64, nil
	}

	share := int(math.MaxInt64) // all fragments
	host := resources.NewHost(resources.ResourceMap(volumeMap), share)
	plans := host.DistributeMultipleRations(required)

	volumePlans := make([][]volumetypes.VolumeMap, len(plans))
	for i, plan := range plans {
		volumePlans[i] = make([]volumetypes.VolumeMap, len(plan))
		for j, resourceMap := range plan {
			volumePlans[i][j] = volumetypes.VolumeMap(resourceMap)
		}
	}

	return len(plans), volumePlans
}

func calculateMonopolyVolumePlan(initVolumeMap volumetypes.VolumeMap, volumeMap volumetypes.VolumeMap, required []int64) (cap int, plans [][]volumetypes.VolumeMap) {
	cap, rawPlans := calculateVolumePlan(volumeMap, required)
	if rawPlans == nil {
		return cap, nil
	}

	scheduled := map[string]bool{}
	for _, plan := range rawPlans {
		if !onSameSource(plan) {
			continue
		}

		volume := plan[0].GetResourceID()
		if _, ok := scheduled[volume]; ok {
			continue
		}

		plans = append(plans, proportionPlan(plan, initVolumeMap[volume]))
		scheduled[volume] = true
	}
	return len(plans), plans
}

func proportionPlan(plan []volumetypes.VolumeMap, size int64) (newPlan []volumetypes.VolumeMap) {
	var total int64
	for _, p := range plan {
		total += p.GetRation()
	}
	for _, p := range plan {
		newRation := int64(math.Floor(float64(p.GetRation()) / float64(total) * float64(size)))
		newPlan = append(newPlan, volumetypes.VolumeMap{p.GetResourceID(): newRation})
	}
	return
}

func distinguishVolumeBindings(vbs volumetypes.VolumeBindings) (norm, mono, unlim volumetypes.VolumeBindings) {
	for _, vb := range vbs {
		switch {
		case vb.RequireScheduleMonopoly():
			mono = append(mono, vb)
		case vb.RequireScheduleUnlimitedQuota():
			unlim = append(unlim, vb)
		case vb.RequireSchedule():
			norm = append(norm, vb)
		}
	}
	return
}

func distinguishAffinityVolumeBindings(vbs volumetypes.VolumeBindings, existing volumetypes.VolumePlan) (requireAff, remain volumetypes.VolumeBindings) {
	for _, vb := range vbs {
		_, _, found := existing.FindAffinityPlan(*vb)
		if found {
			requireAff = append(requireAff, vb)
		} else {
			remain = append(remain, vb)
		}
	}
	return
}

func updateScheduleInfoCapacity(scheduleInfo *volumetypes.ScheduleInfo, capacity int) int {
	if scheduleInfo.Capacity == 0 {
		scheduleInfo.Capacity = capacity
	} else {
		scheduleInfo.Capacity = utils.Min(capacity, scheduleInfo.Capacity)
	}
	return scheduleInfo.Capacity
}

func onSameSource(plan []volumetypes.VolumeMap) bool {
	sourceID := ""
	for _, p := range plan {
		if sourceID == "" {
			sourceID = p.GetResourceID()
		}
		if sourceID != p.GetResourceID() {
			return false
		}
	}
	return true
}
