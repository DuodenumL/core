package calcium

import (
	"context"

	"github.com/projecteru2/core/engine"
	enginetypes "github.com/projecteru2/core/engine/types"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources"
	resourcetypes "github.com/projecteru2/core/resources/types"
	"github.com/projecteru2/core/types"
	"github.com/projecteru2/core/utils"

	"github.com/pkg/errors"
)

// ReallocResource updates workload resource dynamically
func (c *Calcium) ReallocResource(ctx context.Context, opts *types.ReallocOptions) (err error) {
	logger := log.WithField("Calcium", "ReallocResource").WithField("opts", opts)
	workload, err := c.GetWorkload(ctx, opts.ID)
	if err != nil {
		return
	}
	return c.withNodeLocked(ctx, workload.Nodename, func(ctx context.Context, node *types.Node) error {
		return c.withWorkloadLocked(ctx, opts.ID, func(ctx context.Context, workload *types.Workload) error {
			rrs, err := resources.MakeRequests(
				types.ResourceOptions{
					CPUQuotaRequest: utils.Round(workload.CPUQuotaRequest + opts.ResourceOpts.CPUQuotaRequest),
					CPUQuotaLimit:   utils.Round(workload.CPUQuotaLimit + opts.ResourceOpts.CPUQuotaLimit),
					CPUBind:         types.ParseTriOption(opts.CPUBindOpts, len(workload.CPU) > 0),
					CPU:             workload.CPU,
					MemoryRequest:   workload.MemoryRequest + opts.ResourceOpts.MemoryRequest,
					MemoryLimit:     workload.MemoryLimit + opts.ResourceOpts.MemoryLimit,
					StorageRequest:  workload.StorageRequest + opts.ResourceOpts.StorageRequest,
					StorageLimit:    workload.StorageLimit + opts.ResourceOpts.StorageLimit,
					VolumeRequest:   types.MergeVolumeBindings(workload.VolumeRequest, opts.ResourceOpts.VolumeRequest),
					VolumeLimit:     types.MergeVolumeBindings(workload.VolumeLimit, opts.ResourceOpts.VolumeLimit),
					VolumeExist:     workload.VolumePlanRequest,
				},
			)
			if err != nil {
				return logger.Err(ctx, err)
			}
			return logger.Err(ctx, c.doReallocOnNode(ctx, node, workload, rrs))
		})
	})
}

// transaction: node resource
func (c *Calcium) doReallocOnNode(ctx context.Context, node *types.Node, workload *types.Workload, rrs resourcetypes.ResourceRequests) (err error) {
	node.RecycleResources(&workload.ResourceMeta)
	plans, err := resources.SelectNodesByResourceRequests(ctx, rrs, map[string]*types.Node{node.Name: node})
	if err != nil {
		return err
	}

	originalWorkload := *workload
	resourceMeta := &types.ResourceMeta{}
	if err = utils.Txn(
		ctx,

		// if update workload resources
		func(ctx context.Context) (err error) {
			resourceMeta := &types.ResourceMeta{}
			for _, plan := range plans {
				if resourceMeta, err = plan.Dispense(resourcetypes.DispenseOptions{
					Node: node,
				}, resourceMeta); err != nil {
					return err
				}
			}

			return c.doReallocWorkloadsOnInstance(ctx, node.Engine, resourceMeta, workload)
		},
		// then commit changes
		func(ctx context.Context) error {
			for _, plan := range plans {
				plan.ApplyChangesOnNode(node, 0)
			}
			return errors.WithStack(c.store.UpdateNodes(ctx, node))
		},
		// no need rollback
		func(ctx context.Context, failureByCond bool) (err error) {
			if failureByCond {
				return
			}
			r := &types.ResourceMeta{
				CPUQuotaRequest:   originalWorkload.CPUQuotaRequest,
				CPUQuotaLimit:     originalWorkload.CPUQuotaLimit,
				CPU:               originalWorkload.CPU,
				NUMANode:          originalWorkload.NUMANode,
				MemoryRequest:     originalWorkload.MemoryRequest,
				MemoryLimit:       originalWorkload.MemoryLimit,
				VolumeRequest:     originalWorkload.VolumeRequest,
				VolumeLimit:       originalWorkload.VolumeLimit,
				VolumePlanRequest: originalWorkload.VolumePlanRequest,
				VolumePlanLimit:   originalWorkload.VolumePlanLimit,
				VolumeChanged:     resourceMeta.VolumeChanged,
				StorageRequest:    originalWorkload.StorageRequest,
				StorageLimit:      originalWorkload.StorageLimit,
			}
			return c.doReallocWorkloadsOnInstance(ctx, node.Engine, r, workload)
		},

		c.config.GlobalTimeout,
	); err != nil {
		return
	}

	c.doRemapResourceAndLog(ctx, log.WithField("Calcium", "doReallocOnNode"), node)
	return nil
}

func (c *Calcium) doReallocWorkloadsOnInstance(ctx context.Context, engine engine.API, resourceMeta *types.ResourceMeta, workload *types.Workload) (err error) {

	originalWorkload := *workload
	return utils.Txn(
		ctx,

		// if: update workload resources
		func(ctx context.Context) error {
			r := &enginetypes.VirtualizationResource{
				CPU:           resourceMeta.CPU,
				Quota:         resourceMeta.CPUQuotaLimit,
				NUMANode:      resourceMeta.NUMANode,
				Memory:        resourceMeta.MemoryLimit,
				Volumes:       resourceMeta.VolumeLimit.ToStringSlice(false, false),
				VolumePlan:    resourceMeta.VolumePlanLimit.ToLiteral(),
				VolumeChanged: resourceMeta.VolumeChanged,
				Storage:       resourceMeta.StorageLimit,
			}
			return errors.WithStack(engine.VirtualizationUpdateResource(ctx, workload.ID, r))
		},

		// then: update workload meta
		func(ctx context.Context) error {
			workload.CPUQuotaRequest = resourceMeta.CPUQuotaRequest
			workload.CPUQuotaLimit = resourceMeta.CPUQuotaLimit
			workload.CPU = resourceMeta.CPU
			workload.NUMANode = resourceMeta.NUMANode
			workload.MemoryRequest = resourceMeta.MemoryRequest
			workload.MemoryLimit = resourceMeta.MemoryLimit
			workload.VolumeRequest = resourceMeta.VolumeRequest
			workload.VolumePlanRequest = resourceMeta.VolumePlanRequest
			workload.VolumeLimit = resourceMeta.VolumeLimit
			workload.VolumePlanLimit = resourceMeta.VolumePlanLimit
			workload.StorageRequest = resourceMeta.StorageRequest
			workload.StorageLimit = resourceMeta.StorageLimit
			return errors.WithStack(c.store.UpdateWorkload(ctx, workload))
		},

		// rollback: workload meta
		func(ctx context.Context, failureByCond bool) error {
			if failureByCond {
				return nil
			}
			r := &enginetypes.VirtualizationResource{
				CPU:           originalWorkload.CPU,
				Quota:         originalWorkload.CPUQuotaLimit,
				NUMANode:      originalWorkload.NUMANode,
				Memory:        originalWorkload.MemoryLimit,
				Volumes:       originalWorkload.VolumeLimit.ToStringSlice(false, false),
				VolumePlan:    originalWorkload.VolumePlanLimit.ToLiteral(),
				VolumeChanged: resourceMeta.VolumeChanged,
				Storage:       originalWorkload.StorageLimit,
			}
			return errors.WithStack(engine.VirtualizationUpdateResource(ctx, workload.ID, r))
		},

		c.config.GlobalTimeout,
	)
}
