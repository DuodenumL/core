package calcium

import (
	"context"

	enginetypes "github.com/projecteru2/core/engine/types"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources"
	"github.com/projecteru2/core/types"
	"github.com/projecteru2/core/utils"

	"github.com/pkg/errors"
)

// ReallocResource updates workload resource dynamically
func (c *Calcium) ReallocResource(ctx context.Context, opts *types.ReallocOptions) (err error) {
	workload, err := c.GetWorkload(ctx, opts.ID)
	if err != nil {
		return
	}
	// copy origin workload
	originWorkload := *workload
	return c.withNodeLocked(ctx, workload.Nodename, func(ctx context.Context, node *types.Node) error {

		return c.withWorkloadLocked(ctx, opts.ID, func(ctx context.Context, workload *types.Workload) error {
			return c.doReallocOnNode(ctx, node, workload, &originWorkload, opts)
		})
	})
}

func (c *Calcium) doReallocOnNode(ctx context.Context, node *types.Node, workload *types.Workload, originWorkload *types.Workload, opts *types.ReallocOptions) error {
	logger := log.WithField("Calcium", "ReallocResource").WithField("opts", opts)
	return logger.Err(ctx, c.resource.WithNodesLocked(ctx, []string{workload.Nodename}, func(ctx context.Context) error {
		// todo: check availability
		// todo: rollback workload meta

		var resourceArgsMap map[string]map[string]resources.RawParams
		var engineArgsMap map[string]resources.RawParams
		var err error

		return utils.Txn(
			ctx,
			// if: update workload resource
			func(ctx context.Context) error {
				// note here will change the node resource meta (stored in resource plugin)
				engineArgsMap, resourceArgsMap, err = c.resource.Realloc(ctx, []*types.Workload{workload}, resources.RawParams(opts.ResourceOpts))
				if err != nil {
					return err
				}
				return node.Engine.VirtualizationUpdateResource(ctx, opts.ID, &enginetypes.VirtualizationResource{EngineArgs: engineArgsMap[opts.ID]})
			},
			// then: update workload meta
			func(ctx context.Context) error {
				workload.EngineArgs = engineArgsMap[opts.ID]
				workload.ResourceArgs = map[string]map[string]interface{}{}
				for plugin, args := range resourceArgsMap[opts.ID] {
					workload.ResourceArgs[plugin] = args
				}
				return c.store.UpdateWorkload(ctx, workload)
			},
			// rollback: release the resources and rollback workload meta
			func(ctx context.Context, failureByCond bool) error {
				if !failureByCond {
					return nil
				}
				err := c.resource.UpdateNodeResource(ctx, workload.Nodename, []map[string]resources.RawParams{resourceArgsMap[opts.ID]}, resources.Incr)
				if err != nil {
					return errors.WithStack(err)
				}
				return errors.WithStack(c.store.UpdateWorkload(ctx, originWorkload))
			},
			c.config.GlobalTimeout,
		)
	}))
}
