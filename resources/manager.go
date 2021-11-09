package resources

import (
	"context"
	"sync"

	"github.com/projecteru2/core/log"
	resourcetypes "github.com/projecteru2/core/resources/types"
	"github.com/projecteru2/core/types"
	"github.com/projecteru2/core/utils"
)

// PluginManager manages plugins
type PluginManager struct {
	config  types.Config
	plugins []Plugin
}

// NewPluginManager creates a plugin manager
func NewPluginManager(ctx context.Context, config types.Config) *PluginManager {
	pm := &PluginManager{
		config:  config,
		plugins: []Plugin{},
	}
	pm.LoadPlugins(ctx)
	return pm
}

// LoadPlugins .
func (pm *PluginManager) LoadPlugins(ctx context.Context) {
	pm.plugins = []Plugin{}
	if len(pm.config.ResourcePluginsDir) > 0 {
		pluginFiles, err := utils.ListAllExecutableFiles(pm.config.ResourcePluginsDir)
		if err != nil {
			log.Errorf(ctx, "[LoadPlugins] failed to list all executable files dir: %v, err: %v", pm.config.ResourcePluginsDir, err)
			return
		}

		for _, file := range pluginFiles {
			pm.plugins = append(pm.plugins, &BinaryPlugin{path: file, timeout: pm.config.ResourcePluginTimeout})
		}
	}
}

// AddPlugins adds a plugin (for test and debug)
func (pm *PluginManager) AddPlugins(plugins ...Plugin) {
	pm.plugins = append(pm.plugins, plugins...)
}

func (pm *PluginManager) callPlugins(plugins []Plugin, f func(Plugin)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(plugins))

	for _, plugin := range plugins {
		go func(p Plugin) {
			defer wg.Done()
			f(p)
		}(plugin)
	}
	wg.Wait()
}

// LockNodes .
func (pm *PluginManager) LockNodes(ctx context.Context, nodes []string) error {
	locked := make(chan Plugin, len(pm.plugins))

	return utils.Pcr(ctx,
		// prepare: do nothing
		func(ctx context.Context) error {
			return nil
		},
		// commit: try to lock nodes on each plugin
		func(ctx context.Context) error {
			pm.callPlugins(pm.plugins, func(plugin Plugin) {
				if err := plugin.LockNodes(ctx, nodes); err != nil {
					log.Errorf(ctx, "[LockNodes] plugin %v failed to lock nodes %v, err: %v", plugin.Name(), nodes, err)
				} else {
					locked <- plugin
				}
			})
			close(locked)

			if len(locked) != len(pm.plugins) {
				log.Errorf(ctx, "[LockNodes] failed to lock nodes %v, rollback", nodes)
				return types.ErrLockFailed
			}
			log.Infof(ctx, "[LockNodes] locked nodes %v on all plugins", nodes)
			return nil
		},
		// rollback: unlock the locks
		func(ctx context.Context) error {
			rollbackErrChan := make(chan error, len(pm.plugins))
			rollbackPlugins := []Plugin{}
			for plugin := range locked {
				rollbackPlugins = append(rollbackPlugins, plugin)
			}

			pm.callPlugins(rollbackPlugins, func(plugin Plugin) {
				if err := plugin.UnlockNodes(ctx, nodes); err != nil {
					log.Errorf(ctx, "[LockNodes] plugin %v failed to unlock nodes %v, err: %v", plugin.Name(), nodes, err)
					rollbackErrChan <- err
				}
			})
			close(rollbackErrChan)
			if len(rollbackErrChan) > 0 {
				return types.ErrUnlockFailed
			}
			return nil
		},
		pm.config.GlobalTimeout,
	)
}

// UnlockNodes .
func (pm *PluginManager) UnlockNodes(ctx context.Context, nodes []string) error {
	unlocked := make(chan Plugin, len(pm.plugins))

	return utils.Pcr(ctx,
		// prepare: do nothing
		func(ctx context.Context) error {
			return nil
		},
		// commit: try to unlock nodes on each plugin
		func(ctx context.Context) error {
			pm.callPlugins(pm.plugins, func(plugin Plugin) {
				if err := plugin.UnlockNodes(ctx, nodes); err != nil {
					log.Errorf(ctx, "[UnlockNodes] plugin %v failed to unlock nodes %v, err: %v", plugin.Name(), nodes, err)
				} else {
					unlocked <- plugin
				}
			})
			close(unlocked)

			if len(unlocked) != len(pm.plugins) {
				log.Errorf(ctx, "[UnlockNodes] failed to unlock nodes %v on all plugins", nodes)
				return types.ErrUnlockFailed
			}
			log.Infof(ctx, "[UnlockNodes] unlocked nodes %v on all plugins", nodes)
			return nil
		},
		// rollback: do nothing
		func(ctx context.Context) error {
			return nil
		},
		pm.config.GlobalTimeout,
	)
}

// WithNodesLocked .
func (pm *PluginManager) WithNodesLocked(ctx context.Context, nodes []string, f func(context.Context) error) error {
	if err := pm.LockNodes(ctx, nodes); err != nil {
		log.Errorf(ctx, "[WithNodesLocked] failed to lock nodes %v", nodes)
		return err
	}
	defer func() {
		err := pm.UnlockNodes(ctx, nodes)
		if err != nil {
			log.Errorf(ctx, "[WithNodesLocked] failed to unlock nodes %v", nodes)
		}
	}()

	return f(ctx)
}

func (pm *PluginManager) mergeNodeResourceInfo(m1 map[string]*resourcetypes.NodeResourceInfo, m2 map[string]*resourcetypes.NodeResourceInfo) map[string]*resourcetypes.NodeResourceInfo {
	if len(m1) == 0 {
		return m2
	}

	res := map[string]*resourcetypes.NodeResourceInfo{}
	for node, info1 := range m1 {
		// all the capacities should > 0
		if info2, ok := m2[node]; ok {
			res[node] = &resourcetypes.NodeResourceInfo{
				NodeName: node,
				Capacity: utils.Min(info1.Capacity, info2.Capacity),
				Rate:     info1.Rate + info2.Rate*info2.Weight,
				Usage:    info1.Usage + info2.Usage*info2.Weight,
				Weight:   info1.Weight + info2.Weight,
			}
		}
	}
	return res
}

// SelectAvailableNodes returns available nodes which meet all the requirements
// the caller should require locks
// pure calculation
func (pm *PluginManager) SelectAvailableNodes(ctx context.Context, nodes []string, resourceOpts types.RawParams) (map[string]*resourcetypes.NodeResourceInfo, int, error) {
	res := map[string]*resourcetypes.NodeResourceInfo{}
	infoChan := make(chan map[string]*resourcetypes.NodeResourceInfo, len(pm.plugins))

	pm.callPlugins(pm.plugins, func(plugin Plugin) {
		info, _, err := plugin.SelectAvailableNodes(ctx, nodes, resourceOpts)
		if err != nil {
			log.Errorf(ctx, "[SelectAvailableNodes] plugin %v failed to get available nodes, request %v, err %v", plugin.Name(), resourceOpts, err)
		} else {
			infoChan <- info
		}
	})
	close(infoChan)

	if len(infoChan) != len(pm.plugins) {
		return nil, 0, types.ErrGetAvailableNodesFailed
	}

	// get nodes with all resource capacities > 0
	for info := range infoChan {
		res = pm.mergeNodeResourceInfo(res, info)
	}

	total := 0

	// weighted average
	for _, info := range res {
		info.Rate /= info.Weight
		info.Usage /= info.Weight
		total += info.Capacity
	}

	return res, total, nil
}

// mergeEngineArgs e.g. {"file": ["/bin/sh:/bin/sh"], "cpu": 1.2, "cpu-bind": true} + {"file": ["/bin/ls:/bin/ls"], "mem": "1PB"}
// => {"file": ["/bin/sh:/bin/sh", "/bin/ls:/bin/ls"], "cpu": 1.2, "cpu-bind": true, "mem": "1PB"}
func (pm *PluginManager) mergeEngineArgs(ctx context.Context, m1 types.RawParams, m2 types.RawParams) (types.RawParams, error) {
	res := types.RawParams{}
	for key, value := range m1 {
		res[key] = value
	}
	for key, value := range m2 {
		if _, ok := res[key]; ok {
			// only two string slices can be merged
			_, ok1 := res[key].([]string)
			_, ok2 := value.([]string)
			if !ok1 || !ok2 {
				log.Errorf(ctx, "[mergeEngineArgs] only two string slices can be merged! error key %v, m1[key] = %v, m2[key] = %v", key, m1[key], m2[key])
				return nil, types.ErrInvalidEngineArgs
			}
			res[key] = append(res[key].([]string), value.([]string)...)
		} else {
			res[key] = value
		}
	}
	return res, nil
}

// Alloc .
func (pm *PluginManager) Alloc(ctx context.Context, node string, deployCount int, resourceOpts types.RawParams) ([]types.RawParams, []map[string]types.RawParams, error) {
	// engine args for each workload. format: [{"file": ["/bin/sh:/bin/sh"]}]
	resEngineArgs := make([]types.RawParams, deployCount)
	// resource args for each plugin and each workload. format: [{"cpu-plugin": {"cpu": ["1"]}}]
	resResourceArgs := make([]map[string]types.RawParams, deployCount)
	// alloc status map records if the plugins run successfully
	allocStatusMap := map[string]bool{}

	// init engine args
	for i := 0; i < deployCount; i++ {
		resEngineArgs[i] = types.RawParams{}
		resResourceArgs[i] = map[string]types.RawParams{}
	}

	engineArgsChan := make(chan []types.RawParams, len(pm.plugins))
	mutex := &sync.Mutex{}

	return resEngineArgs, resResourceArgs, utils.Pcr(ctx,
		// prepare: calculate engine args and resource args
		func(ctx context.Context) error {
			pm.callPlugins(pm.plugins, func(plugin Plugin) {
				engineArgs, resourceArgs, err := plugin.Alloc(ctx, node, deployCount, resourceOpts)

				mutex.Lock()
				defer mutex.Unlock()

				allocStatusMap[plugin.Name()] = err == nil
				if err != nil {
					log.Errorf(ctx, "[Alloc] plugin %v failed to compute alloc args, request %v, node %v, deploy count %v, err %v", plugin.Name(), resourceOpts, node, deployCount, err)
				} else {
					engineArgsChan <- engineArgs
					for index, resourceArgsMap := range resResourceArgs {
						resourceArgsMap[plugin.Name()] = resourceArgs[index]
					}
				}
			})
			close(engineArgsChan)
			if len(engineArgsChan) != len(pm.plugins) {
				return types.ErrAllocFailed
			}

			// calculate engine args
			var err error
			for engineArgs := range engineArgsChan {
				for index, args := range engineArgs {
					resEngineArgs[index], err = pm.mergeEngineArgs(ctx, resEngineArgs[index], args)
					if err != nil {
						log.Errorf(ctx, "[Alloc] invalid engine args")
						return err
					}
				}
			}
			return nil
		},
		// commit: update node resources
		func(ctx context.Context) error {
			if err := pm.UpdateNodeResourceUsage(ctx, node, resResourceArgs, Incr); err != nil {
				log.Errorf(ctx, "[Alloc] failed to update node resource, err: %v", err)
				return err
			}
			return nil
		},
		// rollback: do nothing
		func(ctx context.Context) error {
			return nil
		},
		pm.config.GlobalTimeout,
	)
}

// Realloc reallocates resource for workloads, returns engine args and final resource args.
func (pm *PluginManager) Realloc(ctx context.Context, node string, originResourceArgs map[string]types.RawParams, resourceOpts types.RawParams) (types.RawParams, map[string]types.RawParams, error) {
	resEngineArgs := types.RawParams{}
	resDeltaResourceArgs := map[string]types.RawParams{}
	resFinalResourceArgs := map[string]types.RawParams{}

	mutex := &sync.Mutex{}
	engineArgsChan := make(chan types.RawParams, len(pm.plugins))
	rollbackPluginChan := make(chan Plugin, len(pm.plugins))

	return resEngineArgs, resFinalResourceArgs, utils.Pcr(ctx,
		// prepare: calculate engine args, delta node resource args and final workload resource args
		func(ctx context.Context) error {
			pm.callPlugins(pm.plugins, func(plugin Plugin) {
				engineArgs, deltaResourceArgs, finalResourceArgs, err := plugin.Realloc(ctx, node, originResourceArgs[plugin.Name()], resourceOpts)
				if err != nil {
					log.Errorf(ctx, "[Realloc] plugin %v failed to calculate realloc args, err: %v", plugin.Name(), err)
				} else {
					mutex.Lock()
					defer mutex.Unlock()

					engineArgsChan <- engineArgs
					resDeltaResourceArgs[plugin.Name()] = deltaResourceArgs
					resFinalResourceArgs[plugin.Name()] = finalResourceArgs
				}
			})

			close(engineArgsChan)
			if len(engineArgsChan) != len(pm.plugins) {
				log.Errorf(ctx, "[Realloc] realloc failed, origin: %+v, opts: %+v", originResourceArgs, resourceOpts)
				return types.ErrReallocFailed
			}

			var err error
			for engineArgs := range engineArgsChan {
				if resEngineArgs, err = pm.mergeEngineArgs(ctx, resEngineArgs, engineArgs); err != nil {
					log.Errorf(ctx, "[Realloc] invalid engine args, err: %v", err)
					return types.ErrReallocFailed
				}
			}
			return nil
		},
		// commit: update node resource
		func(ctx context.Context) error {
			pm.callPlugins(pm.plugins, func(plugin Plugin) {
				if err := plugin.UpdateNodeResourceUsage(ctx, node, []types.RawParams{resDeltaResourceArgs[plugin.Name()]}, Incr); err != nil {
					log.Errorf(ctx, "[Realloc] plugin %v failed to update node resource usage for node %v, err: %v", plugin.Name(), node, err)
				} else {
					rollbackPluginChan <- plugin
				}
			})
			close(rollbackPluginChan)

			if len(rollbackPluginChan) != len(pm.plugins) {
				log.Errorf(ctx, "[Realloc] failed to update node resource usage for node %v, rollback", node)
				return types.ErrRollbackFailed
			}
			return nil
		},
		// rollback: reverts changes
		func(ctx context.Context) error {
			rollbackPlugins := []Plugin{}
			for plugin := range rollbackPluginChan {
				rollbackPlugins = append(rollbackPlugins, plugin)
			}

			errChan := make(chan error, len(rollbackPlugins))
			pm.callPlugins(rollbackPlugins, func(plugin Plugin) {
				if err := plugin.UpdateNodeResourceUsage(ctx, node, []types.RawParams{resDeltaResourceArgs[plugin.Name()]}, Decr); err != nil {
					log.Errorf(ctx, "[Realloc] plugin %v failed to rollback node resource usage for node %v, err: %v", plugin.Name(), node, err)
					errChan <- err
				}
			})
			close(errChan)

			if len(errChan) > 0 {
				return types.ErrRollbackFailed
			}
			return nil
		},
		pm.config.GlobalTimeout,
	)
}

// GetNodeResourceInfo .
func (pm *PluginManager) GetNodeResourceInfo(ctx context.Context, node string, workloads []*types.Workload, fix bool) (map[string]types.RawParams, map[string]types.RawParams, []string, error) {
	resResourceCapacity := map[string]types.RawParams{}
	resResourceUsage := map[string]types.RawParams{}
	resDiffs := []string{}

	diffChan := make(chan []string, len(pm.plugins))
	mutex := &sync.Mutex{}

	pm.callPlugins(pm.plugins, func(plugin Plugin) {
		resourceCapacity, resourceUsage, diffs, err := plugin.GetNodeResourceInfo(ctx, node, workloads, fix)
		if err != nil {
			log.Errorf(ctx, "[GetNodeResourceInfo] plugin %v failed to get node resource of node %v, err: %v", plugin.Name(), node, err)
		} else {
			diffChan <- diffs
			mutex.Lock()
			defer mutex.Unlock()

			resResourceCapacity[plugin.Name()] = resourceCapacity
			resResourceUsage[plugin.Name()] = resourceUsage
		}
	})

	close(diffChan)

	if len(diffChan) != len(pm.plugins) {
		return nil, nil, nil, types.ErrGetNodeResourceFailed
	}

	for diffs := range diffChan {
		resDiffs = append(resDiffs, diffs...)
	}

	return resResourceCapacity, resResourceUsage, resDiffs, nil
}

// UpdateNodeResourceUsage with rollback
func (pm *PluginManager) UpdateNodeResourceUsage(ctx context.Context, node string, resourceArgs []map[string]types.RawParams, direction bool) error {
	resourceArgsMap := map[string][]types.RawParams{}
	rollbackPluginChan := make(chan Plugin, len(pm.plugins))
	errChan := make(chan error, len(pm.plugins))

	return utils.Pcr(ctx,
		// prepare: convert []map[plugin]resourceArgs to map[plugin][]resourceArgs
		// [{"cpu-plugin": {"cpu": 1}}, {"cpu-plugin": {"cpu": 1}}] -> {"cpu-plugin": [{"cpu": 1}, {"cpu": 1}]}
		func(ctx context.Context) error {
			for _, workloadResourceArgs := range resourceArgs {
				for plugin, rawParams := range workloadResourceArgs {
					if _, ok := resourceArgsMap[plugin]; !ok {
						resourceArgsMap[plugin] = []types.RawParams{}
					}
					resourceArgsMap[plugin] = append(resourceArgsMap[plugin], rawParams)
				}
			}
			return nil
		},
		// commit: call plugins to update node resource
		func(ctx context.Context) error {
			pm.callPlugins(pm.plugins, func(plugin Plugin) {
				resourceArgs, ok := resourceArgsMap[plugin.Name()]
				if !ok {
					return
				}

				if err := plugin.UpdateNodeResourceUsage(ctx, node, resourceArgs, direction); err != nil {
					log.Errorf(ctx, "[UpdateNodeResourceUsage] node %v plugin %v failed to update node resource %v, err: %v", node, plugin.Name(), resourceArgs, err)
					errChan <- err
				} else {
					rollbackPluginChan <- plugin
				}
			})
			close(rollbackPluginChan)
			close(errChan)

			if len(errChan) != 0 {
				log.Errorf(ctx, "[UpdateNodeResourceUsage] failed to update node resource for node %v", node)
				return types.ErrUpdateNodeResourceUsageFailed
			}
			return nil
		},
		// rollback: update the rollback resource args in reverse
		func(ctx context.Context) error {
			plugins := []Plugin{}
			for plugin := range rollbackPluginChan {
				plugins = append(plugins, plugin)
			}
			errChan := make(chan error, len(plugins))

			pm.callPlugins(plugins, func(plugin Plugin) {
				resourceArgs := resourceArgsMap[plugin.Name()]
				if err := plugin.UpdateNodeResourceUsage(ctx, node, resourceArgs, !direction); err != nil {
					log.Errorf(ctx, "[UpdateNodeResourceUsage] node %v plugin %v failed to rollback node resource, resourceArgs: %+v, err: %v", resourceArgs, err)
					errChan <- err
				}
			})
			close(errChan)

			if len(errChan) > 0 {
				return types.ErrUpdateNodeResourceUsageFailed
			}
			return nil
		},
		pm.config.GlobalTimeout,
	)
}

// UpdateNodeResourceCapacity updates node resource capacity
// receives resource options instead of resource args
func (pm *PluginManager) UpdateNodeResourceCapacity(ctx context.Context, node string, resourceOpts types.RawParams, direction bool) error {
	rollbackPluginChan := make(chan Plugin, len(pm.plugins))

	return utils.Pcr(ctx,
		// prepare: do nothing
		func(ctx context.Context) error {
			return nil
		},
		// commit: call plugins to update node resource capacity
		func(ctx context.Context) error {
			pm.callPlugins(pm.plugins, func(plugin Plugin) {
				if err := plugin.UpdateNodeResourceCapacity(ctx, node, resourceOpts, direction); err != nil {
					log.Errorf(ctx, "[UpdateNodeResourceCapacity] node %v plugin %v failed to update node resource capacity, opts %+v, err %v", node, plugin.Name(), resourceOpts, err)
				} else {
					rollbackPluginChan <- plugin
				}
			})
			close(rollbackPluginChan)

			if len(rollbackPluginChan) != len(pm.plugins) {
				log.Errorf(ctx, "[UpdateNodeResourceCapacity] node %v opts %+v failed to update node resource capacity, rollback", node, resourceOpts)
				return types.ErrUpdateNodeResourceCapacityFailed
			}
			return nil
		},
		// rollback: update node resource capacity in reverse
		func(ctx context.Context) error {
			// todo: place the logic in a function
			plugins := []Plugin{}
			for plugin := range rollbackPluginChan {
				plugins = append(plugins, plugin)
			}

			errChan := make(chan error, len(plugins))
			pm.callPlugins(plugins, func(plugin Plugin) {
				if err := plugin.UpdateNodeResourceCapacity(ctx, node, resourceOpts, !direction); err != nil {
					log.Errorf(ctx, "[UpdateNodeResourceCapacity] node %v plugin %v failed to rollback node resource capacity, opts %+v, err %v", node, plugin.Name(), resourceOpts, err)
					errChan <- err
				}
			})
			close(errChan)

			if len(errChan) != 0 {
				log.Errorf(ctx, "[UpdateNodeResourceCapacity] failed to rollback")
				return types.ErrRollbackFailed
			}
			return nil
		},
		pm.config.GlobalTimeout,
	)
}

// Remap remaps resource and returns engine args for workloads. format: {"workload-1": {"cpus": ["1-3"]}}
// remap doesn't change resource args
func (pm *PluginManager) Remap(ctx context.Context, node string, workloadMap map[string]*types.Workload) (map[string]types.RawParams, error) {
	resEngineArgsMap := map[string]types.RawParams{}
	engineArgsMapChan := make(chan map[string]types.RawParams, len(pm.plugins))

	// call plugins to remap
	pm.callPlugins(pm.plugins, func(plugin Plugin) {
		engineArgsMap, err := plugin.Remap(ctx, node, workloadMap)
		if err != nil {
			log.Errorf(ctx, "[Remap] plugin %v node %v failed to remap, err: %v", plugin.Name(), node, err)
		} else {
			engineArgsMapChan <- engineArgsMap
		}
	})
	close(engineArgsMapChan)

	if len(engineArgsMapChan) != len(pm.plugins) {
		return nil, types.ErrRemapFailed
	}

	// merge engine args
	var err error
	for engineArgsMap := range engineArgsMapChan {
		for workloadID, engineArgs := range engineArgsMap {
			if _, ok := resEngineArgsMap[workloadID]; !ok {
				resEngineArgsMap[workloadID] = types.RawParams{}
			}
			resEngineArgsMap[workloadID], err = pm.mergeEngineArgs(ctx, resEngineArgsMap[workloadID], engineArgs)
			if err != nil {
				log.Errorf(ctx, "[Remap] invalid engine args")
				return nil, err
			}
		}
	}

	return resEngineArgsMap, nil
}

// AddNode .
func (pm *PluginManager) AddNode(ctx context.Context, node string, resourceOpts types.RawParams) (map[string]types.RawParams, map[string]types.RawParams, error) {
	resResourceCapacity := map[string]types.RawParams{}
	resResourceUsage := map[string]types.RawParams{}

	mutex := &sync.Mutex{}

	return resResourceCapacity, resResourceUsage, utils.Pcr(ctx,
		// prepare: do nothing
		func(ctx context.Context) error {
			return nil
		},
		// commit: call plugins to add the node
		func(ctx context.Context) error {
			pm.callPlugins(pm.plugins, func(plugin Plugin) {
				if resourceCapacity, resourceUsage, err := plugin.AddNode(ctx, node, resourceOpts); err != nil {
					log.Errorf(ctx, "[AddNode] node %v plugin %v failed to add node, req: %v, err: %v", node, plugin.Name(), resourceOpts, err)
				} else {
					mutex.Lock()
					defer mutex.Unlock()

					resResourceCapacity[plugin.Name()] = resourceCapacity
					resResourceUsage[plugin.Name()] = resourceUsage
				}
			})

			if len(resResourceCapacity) != len(pm.plugins) {
				log.Errorf(ctx, "[AddNode] node %v failed to add node %v, rollback", node, resourceOpts)
				return types.ErrAddNodeFailed
			}

			return nil
		},
		// rollback: remove node
		func(ctx context.Context) error {
			errChan := make(chan error, len(pm.plugins))

			pm.callPlugins(pm.plugins, func(plugin Plugin) {
				if _, ok := resResourceCapacity[plugin.Name()]; !ok {
					return
				}
				if err := plugin.RemoveNode(ctx, node); err != nil {
					log.Errorf(ctx, "[AddNode] node %v plugin %v failed to rollback, err: %v", node, plugin.Name(), err)
					errChan <- err
				}
			})
			close(errChan)

			if len(errChan) != 0 {
				return types.ErrRollbackFailed
			}
			return nil
		},
		pm.config.GlobalTimeout,
	)
}

// RemoveNode .
func (pm *PluginManager) RemoveNode(ctx context.Context, node string) error {
	var resourceCapacity map[string]types.RawParams
	var resourceUsage map[string]types.RawParams
	rollbackPluginChan := make(chan Plugin, len(pm.plugins))

	return utils.Pcr(ctx,
		// prepare: get node resource
		func(ctx context.Context) error {
			var err error
			resourceCapacity, resourceUsage, _, err = pm.GetNodeResourceInfo(ctx, node, nil, false)
			if err != nil {
				log.Errorf(ctx, "[RemoveNode] failed to get node %v resource, err: %v", node, err)
				return err
			}
			return nil
		},
		// commit: remove node
		func(ctx context.Context) error {
			pm.callPlugins(pm.plugins, func(plugin Plugin) {
				if err := plugin.RemoveNode(ctx, node); err != nil {
					log.Errorf(ctx, "[AddNode] plugin %v failed to remove node, err: %v", plugin.Name(), node, err)
				} else {
					rollbackPluginChan <- plugin
				}
			})

			close(rollbackPluginChan)
			if len(rollbackPluginChan) != len(pm.plugins) {
				log.Errorf(ctx, "[AddNode] failed to remove node %v", node)
				return types.ErrRemoveNodeFailed
			}
			return nil
		},
		// rollback: add node
		func(ctx context.Context) error {
			plugins := []Plugin{}
			for plugin := range rollbackPluginChan {
				plugins = append(plugins, plugin)
			}
			errChan := make(chan error, len(plugins))

			pm.callPlugins(plugins, func(plugin Plugin) {
				if err := plugin.SetNodeResourceInfo(ctx, resourceCapacity[plugin.Name()], resourceUsage[plugin.Name()]); err != nil {
					log.Errorf(ctx, "[RemoveNode] plugin %v node %v failed to rollback, err: %v", plugin.Name(), node, err)
					errChan <- err
				}
			})
			close(errChan)

			if len(errChan) != 0 {
				log.Errorf(ctx, "[RemoveNode] failed to rollback")
				return types.ErrRollbackFailed
			}
			return nil
		},
		pm.config.GlobalTimeout,
	)
}
