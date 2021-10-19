package calcalcium

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/projecteru2/core/cluster"
	enginetypes "github.com/projecteru2/core/engine/types"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/metrics"
	"github.com/projecteru2/core/resources"
	"github.com/projecteru2/core/strategy"
	"github.com/projecteru2/core/types"
	"github.com/projecteru2/core/utils"
	"github.com/projecteru2/core/wal"

	"github.com/pkg/errors"
	"github.com/sanity-io/litter"
)

// CreateWorkload use options to create workloads
func (c *Calcium) CreateWorkload(ctx context.Context, opts *types.DeployOptions) (chan *types.CreateWorkloadMessage, error) {
	logger := log.WithField("Calcium", "CreateWorkload").WithField("opts", opts)
	if err := opts.Validate(); err != nil {
		return nil, logger.Err(ctx, err)
	}

	opts.ProcessIdent = utils.RandomString(16)
	log.Infof(ctx, "[CreateWorkload %s] Creating workload with options:\n%s", opts.ProcessIdent, litter.Sdump(opts))
	// Count 要大于0
	if opts.Count <= 0 {
		return nil, logger.Err(ctx, errors.WithStack(types.NewDetailedErr(types.ErrBadCount, opts.Count)))
	}

	return c.doCreateWorkloads(ctx, opts), nil
}

func (c *Calcium) doCreateWorkloads(ctx context.Context, opts *types.DeployOptions) chan *types.CreateWorkloadMessage {
	logger := log.WithField("Calcium", "doCreateWorkloads").WithField("opts", opts)
	ch := make(chan *types.CreateWorkloadMessage)
	// map[node][]engineArgs
	engineArgsMap := map[string][]resources.RawParams{}
	// map[node][]map[plugin]resourceArgs
	resourceArgsMap := map[string][]map[string]resources.RawParams{}
	var (
		deployMap   map[string]int
		rollbackMap map[string][]int
	)

	utils.SentryGo(func() {
		defer func() {
			cctx, cancel := context.WithTimeout(utils.InheritTracingInfo(ctx, context.TODO()), c.config.GlobalTimeout)
			for nodename := range deployMap {
				if e := c.store.DeleteProcessing(cctx, opts.GetProcessing(nodename)); e != nil {
					logger.Errorf(ctx, "[doCreateWorkloads] delete processing failed for %s: %+v", nodename, e)
				}
			}
			close(ch)
			cancel()
		}()

		_ = utils.Txn(
			ctx,

			// if: alloc resources
			func(ctx context.Context) (err error) {
				defer func() {
					if err != nil {
						ch <- &types.CreateWorkloadMessage{Error: logger.Err(ctx, err)}
					}
				}()
				return c.withNodesLocked(ctx, opts.NodeFilter, func(ctx context.Context, nodeMap map[string]*types.Node) (err error) {
					nodes := []string{}
					for node := range nodeMap {
						nodes = append(nodes, node)
					}

					return c.resource.WithNodesLocked(ctx, nodes, func(ctx context.Context) error {
						// get nodes with capacity > 0
						nodeResourceInfoMap, total, err := c.resource.GetAvailableNodes(ctx, resources.RawParams(opts.ResourceOpts))
						if err != nil {
							return err
						}

						// get deployed & processing workload count on each node
						deployStatusMap, err := c.store.GetDeployStatus(ctx, opts.Name, opts.Entrypoint.Name)
						if err != nil {
							return err
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
						deployMap, err = strategy.Deploy(ctx, opts, strategyInfos, total)
						if err != nil {
							return err
						}

						// commit changes
						for node, deploy := range deployMap {
							engineArgsMap[node], resourceArgsMap[node], err = c.resource.Alloc(ctx, node, deploy, resources.RawParams(opts.ResourceOpts))
							if err = c.store.CreateProcessing(ctx, opts.GetProcessing(node), deploy); err != nil {
								return errors.WithStack(err)
							}
						}
						return nil
					})
				})
			},

			// then: deploy workloads
			func(ctx context.Context) (err error) {
				rollbackMap, err = c.doDeployWorkloads(ctx, ch, opts, engineArgsMap, resourceArgsMap, deployMap)
				return err
			},

			// rollback: give back resources
			func(ctx context.Context, failedOnCond bool) (err error) {
				if failedOnCond {
					return
				}
				for nodename, rollbackIndices := range rollbackMap {
					if e := c.withNodeLocked(ctx, nodename, func(ctx context.Context, node *types.Node) error {
						return c.resource.WithNodesLocked(ctx, []string{nodename}, func(ctx context.Context) error {
							resourceArgsToRollback := []map[string]resources.RawParams{}
							for _, idx := range rollbackIndices {
								resourceArgsToRollback = append(resourceArgsToRollback, resourceArgsMap[nodename][idx])
							}
							return c.resource.UpdateNodeResource(ctx, nodename, resourceArgsToRollback, resources.Incr)
						})
					}); e != nil {
						err = logger.Err(ctx, e)
					}
				}
				return err
			},

			c.config.GlobalTimeout,
		)
	})

	return ch
}

func (c *Calcium) doDeployWorkloads(ctx context.Context,
	ch chan *types.CreateWorkloadMessage,
	opts *types.DeployOptions,
	engineArgsMap map[string][]resources.RawParams,
	resourceArgsMap map[string][]map[string]resources.RawParams,
	deployMap map[string]int) (_ map[string][]int, err error) {

	wg := sync.WaitGroup{}
	wg.Add(len(deployMap))
	syncRollbackMap := sync.Map{}

	seq := 0
	rollbackMap := make(map[string][]int)
	for nodename, deploy := range deployMap {
		utils.SentryGo(func(deploy int) func() {
			return func() {
				metrics.Client.SendDeployCount(deploy)
			}
		}(deploy))
		utils.SentryGo(func(nodename string, deploy, seq int) func() {
			return func() {
				defer wg.Done()
				if indices, err := c.doDeployWorkloadsOnNode(ctx, ch, nodename, opts, deploy, engineArgsMap[nodename], resourceArgsMap[nodename], seq); err != nil {
					syncRollbackMap.Store(nodename, indices)
				}
			}
		}(nodename, deploy, seq))

		seq += deploy
	}

	wg.Wait()
	syncRollbackMap.Range(func(key, value interface{}) bool {
		nodename := key.(string)
		indices := value.([]int)
		rollbackMap[nodename] = indices
		return true
	})
	log.Debugf(ctx, "[doDeployWorkloads] rollbackMap: %+v", rollbackMap)
	if len(rollbackMap) != 0 {
		err = types.ErrRollbackMapIsNotEmpty
	}
	return rollbackMap, err
}

// deploy scheduled workloads on one node
func (c *Calcium) doDeployWorkloadsOnNode(ctx context.Context, ch chan *types.CreateWorkloadMessage, nodename string, opts *types.DeployOptions, deploy int, engineArgs []resources.RawParams, resourceArgs []map[string]resources.RawParams, seq int) (indices []int, err error) {
	logger := log.WithField("Calcium", "doDeployWorkloadsOnNode").WithField("nodename", nodename).WithField("opts", opts).WithField("deploy", deploy).WithField("engineArgs", engineArgs).WithField("resourceArgs", resourceArgs).WithField("seq", seq)
	node, err := c.doGetAndPrepareNode(ctx, nodename, opts.Image)
	if err != nil {
		for i := 0; i < deploy; i++ {
			ch <- &types.CreateWorkloadMessage{Error: logger.Err(ctx, err)}
		}
		return utils.Range(deploy), err
	}

	pool, appendLock := utils.NewGoroutinePool(int(c.config.MaxConcurrency)), sync.Mutex{}
	for idx := 0; idx < deploy; idx++ {
		createMsg := &types.CreateWorkloadMessage{
			Podname:  opts.Podname,
			Nodename: nodename,
			Publish:  map[string][]string{},
		}

		pool.Go(ctx, func(idx int) func() {
			return func() {
				var e error
				defer func() {
					if e != nil {
						err = e
						createMsg.Error = logger.Err(ctx, e)
						appendLock.Lock()
						indices = append(indices, idx)
						appendLock.Unlock()
					}
					ch <- createMsg
				}()

				createMsg.EngineArgs = types.RawParams(engineArgs[idx])
				createMsg.ResourceArgs = map[string]types.RawParams{}
				for k, v := range resourceArgs[idx] {
					createMsg.ResourceArgs[k] = types.RawParams(v)
				}

				createOpts := c.doMakeWorkloadOptions(ctx, seq+idx, createMsg, opts, node)
				e = c.doDeployOneWorkload(ctx, node, opts, createMsg, createOpts, true)
			}
		}(idx))
	}
	pool.Wait(ctx)

	// remap 就不搞进事务了吧, 回滚代价太大了
	// 放任 remap 失败的后果是, share pool 没有更新, 这个后果姑且认为是可以承受的
	// 而且 remap 是一个幂等操作, 就算这次 remap 失败, 下次 remap 也能收敛到正确到状态
	if err := c.withNodeLocked(ctx, nodename, func(ctx context.Context, node *types.Node) error {
		c.doRemapResourceAndLog(ctx, logger, node)
		return nil
	}); err != nil {
		logger.Errorf(ctx, "failed to lock node to remap: %v", err)
	}
	return indices, err
}

func (c *Calcium) doGetAndPrepareNode(ctx context.Context, nodename, image string) (*types.Node, error) {
	node, err := c.GetNode(ctx, nodename)
	if err != nil {
		return nil, err
	}

	return node, pullImage(ctx, node, image)
}

// transaction: workload metadata consistency
func (c *Calcium) doDeployOneWorkload(
	ctx context.Context,
	node *types.Node,
	opts *types.DeployOptions,
	msg *types.CreateWorkloadMessage,
	config *enginetypes.VirtualizationCreateOptions,
	decrProcessing bool,
) (err error) {
	workload := &types.Workload{
		ResourceArgs: map[string]map[string]interface{}{},
		EngineArgs:   msg.EngineArgs,
		Name:         config.Name,
		Labels:       config.Labels,
		Podname:      opts.Podname,
		Nodename:     node.Name,
		Hook:         opts.Entrypoint.Hook,
		Privileged:   opts.Entrypoint.Privileged,
		Engine:       node.Engine,
		Image:        opts.Image,
		Env:          opts.Env,
		User:         opts.User,
		CreateTime:   time.Now().Unix(),
	}

	// copy resource args
	for k, v := range msg.ResourceArgs {
		workload.ResourceArgs[k] = v
	}

	var commit wal.Commit
	defer func() {
		if commit != nil {
			if err := commit(); err != nil {
				log.Errorf(ctx, "[doDeployOneWorkload] Commit WAL %s failed: %v", eventCreateWorkload, err)
			}
		}
	}()
	return utils.Txn(
		ctx,
		// create workload
		func(ctx context.Context) error {
			created, err := node.Engine.VirtualizationCreate(ctx, config)
			if err != nil {
				return errors.WithStack(err)
			}
			workload.ID = created.ID
			// We couldn't WAL the workload ID above VirtualizationCreate temporarily,
			// so there's a time gap window, once the core process crashes between
			// VirtualizationCreate and logCreateWorkload then the workload is leaky.
			if commit, err = c.wal.logCreateWorkload(workload.ID, node.Name); err != nil {
				return err
			}
			return nil
		},

		func(ctx context.Context) (err error) {
			// avoid to be interrupted by MakeDeployStatus
			processing := opts.GetProcessing(node.Name)
			if !decrProcessing {
				processing = nil
			}
			// add workload metadata first
			if err := c.store.AddWorkload(ctx, workload, processing); err != nil {
				return errors.WithStack(err)
			}
			log.Infof(ctx, "[doDeployOneWorkload] workload %s metadata created", workload.ID)

			// Copy data to workload
			if len(opts.Files) > 0 {
				for _, file := range opts.Files {
					if err = c.doSendFileToWorkload(ctx, node.Engine, workload.ID, file); err != nil {
						return err
					}
				}
			}

			// deal with hook
			if len(opts.AfterCreate) > 0 {
				if workload.Hook != nil {
					workload.Hook = &types.Hook{
						AfterStart: append(opts.AfterCreate, workload.Hook.AfterStart...),
						Force:      workload.Hook.Force,
					}
				} else {
					workload.Hook = &types.Hook{
						AfterStart: opts.AfterCreate,
						Force:      opts.IgnoreHook,
					}
				}
			}

			// start workload
			msg.Hook, err = c.doStartWorkload(ctx, workload, opts.IgnoreHook)
			if err != nil {
				return err
			}

			// reset workload.hook
			workload.Hook = opts.Entrypoint.Hook

			// inspect real meta
			var workloadInfo *enginetypes.VirtualizationInfo
			workloadInfo, err = workload.Inspect(ctx) // 补充静态元数据
			if err != nil {
				return err
			}

			// update meta
			if workloadInfo.Networks != nil {
				msg.Publish = utils.MakePublishInfo(workloadInfo.Networks, opts.Entrypoint.Publish)
			}

			// if workload metadata changed, then update
			if workloadInfo.User != workload.User {
				// reset users
				workload.User = workloadInfo.User

				if err := c.store.UpdateWorkload(ctx, workload); err != nil {
					return errors.WithStack(err)
				}
				log.Infof(ctx, "[doDeployOneWorkload] workload %s metadata updated", workload.ID)
			}

			msg.WorkloadID = workload.ID
			msg.WorkloadName = workload.Name
			msg.Podname = workload.Podname
			msg.Nodename = workload.Nodename
			return nil
		},

		// remove workload
		func(ctx context.Context, _ bool) error {
			log.Errorf(ctx, "[doDeployOneWorkload] failed to deploy workload %s, rollback", workload.ID)
			if workload.ID == "" {
				return nil
			}

			if err := c.store.RemoveWorkload(ctx, workload); err != nil {
				log.Errorf(ctx, "[doDeployOneWorkload] failed to remove workload %s")
			}

			return workload.Remove(ctx, true)
		},
		c.config.GlobalTimeout,
	)
}

func (c *Calcium) doMakeWorkloadOptions(ctx context.Context, no int, msg *types.CreateWorkloadMessage, opts *types.DeployOptions, node *types.Node) *enginetypes.VirtualizationCreateOptions {
	config := &enginetypes.VirtualizationCreateOptions{}
	// general
	config.EngineArgs = msg.EngineArgs
	config.RawArgs = opts.RawArgs
	config.Lambda = opts.Lambda
	config.User = opts.User
	config.DNS = opts.DNS
	config.Image = opts.Image
	config.Stdin = opts.OpenStdin
	config.Hosts = opts.ExtraHosts
	config.Debug = opts.Debug
	config.Networks = opts.Networks

	// resource args
	config.ResourceArgs = map[string]map[string]interface{}{}
	for k, v := range msg.ResourceArgs {
		config.ResourceArgs[k] = v
	}

	// entry
	entry := opts.Entrypoint
	config.WorkingDir = entry.Dir
	config.Privileged = entry.Privileged
	config.Sysctl = entry.Sysctls
	config.Publish = entry.Publish
	config.Restart = entry.Restart
	if entry.Log != nil {
		config.LogType = entry.Log.Type
		config.LogConfig = entry.Log.Config
	}
	// name
	suffix := utils.RandomString(6)
	config.Name = utils.MakeWorkloadName(opts.Name, opts.Entrypoint.Name, suffix)
	msg.WorkloadName = config.Name
	// command and user
	// extra args is dynamically
	config.Cmd = opts.Entrypoint.Commands
	// env
	env := append(opts.Env, fmt.Sprintf("APP_NAME=%s", opts.Name)) // nolint
	env = append(env, fmt.Sprintf("ERU_POD=%s", opts.Podname))
	env = append(env, fmt.Sprintf("ERU_NODE_NAME=%s", node.Name))
	env = append(env, fmt.Sprintf("ERU_WORKLOAD_SEQ=%d", no))
	config.Env = env
	// basic labels, bind to LabelMeta
	config.Labels = map[string]string{
		cluster.ERUMark: "1",
		cluster.LabelMeta: utils.EncodeMetaInLabel(ctx, &types.LabelMeta{
			Publish:     opts.Entrypoint.Publish,
			HealthCheck: entry.HealthCheck,
		}),
		cluster.LabelNodeName: node.Name,
		cluster.LabelCoreID:   c.identifier,
	}
	for key, value := range opts.Labels {
		config.Labels[key] = value
	}

	return config
}
