package models

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/projecteru2/core/resources/volume/schedule"
	"github.com/projecteru2/core/resources/volume/types"
)

// GetDeployArgs .
func (v *Volume) GetDeployArgs(ctx context.Context, node string, deployCount int, opts *types.WorkloadResourceOpts) ([]*types.EngineArgs, []*types.WorkloadResourceArgs, error) {
	if err := opts.Validate(); err != nil {
		logrus.Errorf("[Alloc] invalid resource opts %+v, err: %v", opts, err)
		return nil, nil, err
	}

	resourceInfo, err := v.doGetNodeResourceInfo(ctx, node)
	if err != nil {
		logrus.Errorf("[Alloc] failed to get resource info of node %v, err: %v", node, err)
		return nil, nil, err
	}

	return v.doAlloc(ctx, resourceInfo, node, deployCount, opts)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func getVolumePlanLimit(bindings types.VolumeBindings, volumePlan types.VolumePlan) types.VolumePlan {
	volumeBindingToVolumeMap := map[[3]string]types.VolumeMap{}
	for binding, volumeMap := range volumePlan {
		volumeBindingToVolumeMap[binding.GetMapKey()] = volumeMap
	}

	volumePlanLimit := types.VolumePlan{}

	for _, binding := range bindings {
		if volumeMap, ok := volumeBindingToVolumeMap[binding.GetMapKey()]; ok {
			volumePlanLimit[*binding] = types.VolumeMap{volumeMap.GetResourceID(): maxInt64(binding.SizeInBytes, volumeMap.GetRation())}
		}
	}
	return volumePlanLimit
}

func (v *Volume) doAlloc(ctx context.Context, resourceInfo *types.NodeResourceInfo, node string, deployCount int, opts *types.WorkloadResourceOpts) ([]*types.EngineArgs, []*types.WorkloadResourceArgs, error) {
	_, volumePlansMap, total, err := schedule.Schedule(ctx, []*types.NodeResourceInfo{resourceInfo}, []string{node}, opts)
	if err != nil {
		logrus.Errorf("[doAlloc] failed to schedule, err: %v", err)
		return nil, nil, err
	}

	if total < deployCount {
		return nil, nil, types.ErrInsufficientResource
	}

	volumePlans := []types.VolumePlan{}
	for _, plans := range volumePlansMap {
		volumePlans = append(volumePlans, plans...)
	}

	resEngineArgs := []*types.EngineArgs{}
	resResourceArgs := []*types.WorkloadResourceArgs{}

	// if volume is not required
	if len(volumePlans) == 0 {
		for i := 0; i < deployCount; i++ {
			resEngineArgs = append(resEngineArgs, &types.EngineArgs{
				Storage: opts.StorageLimit,
			})
			resResourceArgs = append(resResourceArgs, &types.WorkloadResourceArgs{
				StorageRequest: opts.StorageRequest,
				StorageLimit:   opts.StorageLimit,
			})
		}
		return resEngineArgs, resResourceArgs, nil
	}

	volumePlans = volumePlans[:deployCount]
	volumeSizeLimitMap := map[*types.VolumeBinding]int64{}
	for _, binding := range opts.VolumesLimit {
		volumeSizeLimitMap[binding] = binding.SizeInBytes
	}

	for _, volumePlan := range volumePlans {
		engineArgs := &types.EngineArgs{Storage: opts.StorageLimit}
		for _, binding := range opts.VolumesLimit.ApplyPlan(volumePlan) {
			engineArgs.Volumes = append(engineArgs.Volumes, binding.ToString(true))
		}

		resourceArgs := &types.WorkloadResourceArgs{
			VolumesRequest:    opts.VolumesRequest,
			VolumesLimit:      opts.VolumesLimit,
			VolumePlanRequest: volumePlan,
			VolumePlanLimit:   getVolumePlanLimit(opts.VolumesLimit, volumePlan),
			StorageRequest:    opts.StorageRequest,
			StorageLimit:      opts.StorageLimit,
		}

		resEngineArgs = append(resEngineArgs, engineArgs)
		resResourceArgs = append(resResourceArgs, resourceArgs)
	}

	return resEngineArgs, resResourceArgs, nil
}
