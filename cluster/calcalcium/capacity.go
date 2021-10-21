package calcalcium

import (
	"context"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources"
	resourcetypes "github.com/projecteru2/core/resources/types"
	"github.com/projecteru2/core/strategy"
	"github.com/projecteru2/core/types"

	"github.com/pkg/errors"
)

// CalculateCapacity calculates capacity
func (c *Calcium) CalculateCapacity(ctx context.Context, opts *types.DeployOptions) (*types.CapacityMessage, error) {
	logger := log.WithField("Calcium", "CalculateCapacity").WithField("opts", opts)
	var err error
	msg := &types.CapacityMessage{
		Total:          0,
		NodeCapacities: map[string]int{},
	}

	return msg, c.withNodesLocked(ctx, opts.NodeFilter, func(ctx context.Context, nodeMap map[string]*types.Node) error {
		nodes := []string{}
		for node := range nodeMap {
			nodes = append(nodes, node)
		}

		if opts.DeployStrategy != strategy.Dummy {
			if msg.NodeCapacities, err = c.doGetDeployMap(ctx, nodes, opts); err != nil {
				logger.Errorf(ctx, "[Calcium.CalculateCapacity] doGetDeployMap failed: %+v", err)
				return err
			}

			for _, capacity := range msg.NodeCapacities {
				msg.Total += capacity
			}
		} else {
			var infos map[string]*resourcetypes.NodeResourceInfo
			infos, msg.Total, err = c.doCalculateCapacity(ctx, nodes, opts)
			if err != nil {
				logger.Errorf(ctx, "[Calcium.CalculateCapacity] doCalculateCapacity failed: %+v", err)
				return err
			}
			for node, info := range infos {
				msg.NodeCapacities[node] = info.Capacity
			}
		}
		return nil
	})
}

func (c *Calcium) doCalculateCapacity(ctx context.Context, nodes []string, opts *types.DeployOptions) (
	nodeResourceInfoMap map[string]*resourcetypes.NodeResourceInfo,
	total int,
	err error,
) {
	if len(nodes) == 0 {
		return nil, 0, errors.WithStack(types.ErrInsufficientNodes)
	}

	// get nodes with capacity > 0
	nodeResourceInfoMap, total, err = c.resource.SelectAvailableNodes(ctx, nodes, resources.RawParams(opts.ResourceOpts))
	if err != nil {
		log.Errorf(ctx, "[doGetDeployMap] failed to get available nodes,")
		return nil, 0, err
	}

	return nodeResourceInfoMap, total, nil
}
