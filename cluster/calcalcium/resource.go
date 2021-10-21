package calcalcium

import (
	"context"
	"fmt"

	enginetypes "github.com/projecteru2/core/engine/types"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources"
	"github.com/projecteru2/core/strategy"
	"github.com/projecteru2/core/types"
	"github.com/projecteru2/core/utils"
)

// PodResource show pod resource usage
func (c *Calcium) PodResource(ctx context.Context, podname string) (*types.PodResource, error) {
	logger := log.WithField("Calcium", "PodResource").WithField("podname", podname)
	nodes, err := c.ListPodNodes(ctx, podname, nil, true)
	if err != nil {
		return nil, logger.Err(ctx, err)
	}
	r := &types.PodResource{
		Name:          podname,
		NodesResource: []*types.NodeResource{},
	}
	for _, node := range nodes {
		nodeResource, err := c.doGetNodeResource(ctx, node.Name, false)
		if err != nil {
			return nil, logger.Err(ctx, err)
		}
		r.NodesResource = append(r.NodesResource, nodeResource)
	}
	return r, nil
}

// NodeResource check node's workload and resource
func (c *Calcium) NodeResource(ctx context.Context, nodename string, fix bool) (*types.NodeResource, error) {
	logger := log.WithField("Calcium", "NodeResource").WithField("nodename", nodename).WithField("fix", fix)
	if nodename == "" {
		return nil, logger.Err(ctx, types.ErrEmptyNodeName)
	}

	nr, err := c.doGetNodeResource(ctx, nodename, fix)
	if err != nil {
		return nil, logger.Err(ctx, err)
	}
	for _, workload := range nr.Workloads {
		if _, err := workload.Inspect(ctx); err != nil { // 用于探测节点上容器是否存在
			nr.Diffs = append(nr.Diffs, fmt.Sprintf("workload %s inspect failed %v \n", workload.ID, err))
			continue
		}
	}
	return nr, logger.Err(ctx, err)
}

func (c *Calcium) doGetNodeResource(ctx context.Context, nodename string, fix bool) (*types.NodeResource, error) {
	var nr *types.NodeResource
	return nr, c.withNodeLocked(ctx, nodename, func(ctx context.Context, node *types.Node) error {
		return c.resource.WithNodesLocked(ctx, []string{nodename}, func(ctx context.Context) error {
			workloads, err := c.ListNodeWorkloads(ctx, nodename, nil)
			if err != nil {
				log.Errorf(ctx, "[doGetNodeResource] failed to list node workloads, node %v, err: %v", nodename, err)
				return err
			}

			resourceArgs, diffs, err := c.resource.GetNodeResource(ctx, nodename, workloads, fix)
			if err != nil {
				log.Errorf(ctx, "[doGetNodeResource] failed to get node resource, node %v, err: %v", nodename, err)
				return err
			}

			nr = &types.NodeResource{
				Name:         nodename,
				ResourceArgs: map[string]types.RawParams{},
				Diffs:        diffs,
			}

			for plugin, args := range resourceArgs {
				nr.ResourceArgs[plugin] = types.RawParams(args)
			}
			return nil
		})
	})
}
func (c *Calcium) doGetDeployMap(ctx context.Context, nodes []string, opts *types.DeployOptions) (map[string]int, error) {
	// get nodes with capacity > 0
	nodeResourceInfoMap, total, err := c.resource.SelectAvailableNodes(ctx, nodes, resources.RawParams(opts.ResourceOpts))
	if err != nil {
		log.Errorf(ctx, "[doGetDeployMap] failed to select available nodes, nodes %v, err %v", nodes, err)
		return nil, err
	}

	// get deployed & processing workload count on each node
	deployStatusMap, err := c.store.GetDeployStatus(ctx, opts.Name, opts.Entrypoint.Name)
	if err != nil {
		log.Errorf(ctx, "failed to get deploy status for %v_%v, err %v", opts.Name, opts.Entrypoint.Name, err)
		return nil, err
	}

	// generate strategy info
	strategyInfos := []strategy.Info{}
	for node, resourceInfo := range nodeResourceInfoMap {
		strategyInfos = append(strategyInfos, strategy.Info{
			Nodename: node,
			Usage:    resourceInfo.Usage,
			Rate:     resourceInfo.Rate,
			Capacity: resourceInfo.Capacity,
			Count:    deployStatusMap[node],
		})
	}

	// generate deploy plan
	deployMap, err := strategy.Deploy(ctx, opts, strategyInfos, total)
	if err != nil {
		return nil, err
	}

	return deployMap, nil
}

type remapMsg struct {
	id  string
	err error
}

// called on changes of resource binding, such as cpu binding
// as an internal api, remap doesn't lock node, the responsibility of that should be taken on by caller
func (c *Calcium) remapResource(ctx context.Context, node *types.Node) (ch chan *remapMsg, err error) {
	workloads, err := c.store.ListNodeWorkloads(ctx, node.Name, nil)
	if err != nil {
		return
	}

	workloadMap := map[string]*types.Workload{}
	for _, workload := range workloads {
		workloadMap[workload.ID] = workload
	}

	engineArgsMap, err := c.resource.Remap(ctx, node.Name, workloadMap)
	if err != nil {
		return nil, err
	}

	ch = make(chan *remapMsg, len(engineArgsMap))
	go func() {
		defer close(ch)
		for workloadID, engineArgs := range engineArgsMap {
			ch <- &remapMsg{
				id:  workloadID,
				err: node.Engine.VirtualizationUpdateResource(ctx, workloadID, &enginetypes.VirtualizationResource{EngineArgs: engineArgs}),
			}
		}
	}()

	return ch, nil
}

func (c *Calcium) doRemapResourceAndLog(ctx context.Context, logger log.Fields, node *types.Node) {
	log.Debugf(ctx, "[doRemapResourceAndLog] remap node %s", node.Name)
	ctx, cancel := context.WithTimeout(utils.InheritTracingInfo(ctx, context.TODO()), c.config.GlobalTimeout)
	defer cancel()
	logger = logger.WithField("Calcium", "doRemapResourceAndLog").WithField("nodename", node.Name)
	if ch, err := c.remapResource(ctx, node); logger.Err(ctx, err) == nil {
		for msg := range ch {
			log.Infof(ctx, "[doRemapResourceAndLog] id %v", msg.id)
			logger.WithField("id", msg.id).Err(ctx, msg.err) // nolint:errcheck
		}
	}
}
