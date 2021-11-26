package resources

import (
	"context"
	"math"
	"sync"

	"github.com/projecteru2/core/log"
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
			log.Infof(ctx, "[LoadPlugins] load binary plugin: %v", file)
			pm.plugins = append(pm.plugins, &BinaryPlugin{path: file, timeout: pm.config.ResourcePluginsTimeout})
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

func callPlugins[T any](plugins []Plugin, f func(Plugin) (T, error)) (results map[Plugin]T) {
	resMap := sync.Map{}
	wg := &sync.WaitGroup{}
	wg.Add(len(plugins))

	for _, plugin := range plugins {
		go func(p Plugin) {
			defer wg.Done()
			if res, err := f(p); err == nil {
				resMap.Store(p, res)
			}
		}(plugin)
	}
	wg.Wait()

	results = map[Plugin]T{}
	resMap.Range(func(key, value interface{}) bool {
		plugin := key.(Plugin)
		res := value.(T)
		results[plugin] = res
		return true
	})

	return results
}

func (pm *PluginManager) mergeNodeResourceInfo(m1 map[string]*NodeCapacityInfo, m2 map[string]*NodeCapacityInfo) map[string]*NodeCapacityInfo {
	if len(m1) == 0 {
		return m2
	}

	res := map[string]*NodeCapacityInfo{}
	for node, info1 := range m1 {
		// all the capacities should > 0
		if info2, ok := m2[node]; ok {
			res[node] = &NodeCapacityInfo{
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

// GetNodesCapacity returns available nodes which meet all the requirements
// the caller should require locks
// pure calculation
func (pm *PluginManager) GetNodesCapacity(ctx context.Context, nodeNames []string, resourceOpts types.WorkloadResourceOpts) (map[string]*NodeCapacityInfo, int, error) {
	res := map[string]*NodeCapacityInfo{}

	respMap := callPlugins(pm.plugins, func(plugin Plugin) (*GetNodesCapacityResponse, error) {
		resp, err := plugin.GetNodesCapacity(ctx, nodeNames, resourceOpts)
		if err != nil {
			log.Errorf(ctx, "[GetNodesCapacity] plugin %v failed to get available nodeNames, request %v, err %v", plugin.Name(), resourceOpts, err)
		}
		return resp, err
	})

	if len(respMap) != len(pm.plugins) {
		return nil, 0, types.ErrGetAvailableNodesFailed
	}

	// get nodeNames with all resource capacities > 0
	for _, infoMap := range respMap {
		res = pm.mergeNodeResourceInfo(res, infoMap.Nodes)
	}

	total := 0

	// weighted average
	for _, info := range res {
		info.Rate /= info.Weight
		info.Usage /= info.Weight
		if info.Capacity == math.MaxInt {
			total = math.MaxInt
		} else {
			total += info.Capacity
		}
	}

	return res, total, nil
}

// mergeEngineArgs e.g. {"file": ["/bin/sh:/bin/sh"], "cpu": 1.2, "cpu-bind": true} + {"file": ["/bin/ls:/bin/ls"], "mem": "1PB"}
// => {"file": ["/bin/sh:/bin/sh", "/bin/ls:/bin/ls"], "cpu": 1.2, "cpu-bind": true, "mem": "1PB"}
func (pm *PluginManager) mergeEngineArgs(ctx context.Context, m1 types.EngineArgs, m2 types.EngineArgs) (types.EngineArgs, error) {
	res := types.EngineArgs{}
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
func (pm *PluginManager) Alloc(ctx context.Context, nodeName string, deployCount int, resourceOpts types.WorkloadResourceOpts) ([]types.EngineArgs, []map[string]types.WorkloadResourceArgs, error) {
	resEngineArgs := make([]types.EngineArgs, deployCount)
	resResourceArgs := make([]map[string]types.WorkloadResourceArgs, deployCount)

	// init engine args
	for i := 0; i < deployCount; i++ {
		resEngineArgs[i] = types.EngineArgs{}
		resResourceArgs[i] = map[string]types.WorkloadResourceArgs{}
	}

	return resEngineArgs, resResourceArgs, utils.Pcr(ctx,
		// prepare: calculate engine args and resource args
		func(ctx context.Context) error {
			respMap := callPlugins(pm.plugins, func(plugin Plugin) (*AllocResponse, error) {
				resp, err := plugin.Alloc(ctx, nodeName, deployCount, resourceOpts)
				if err != nil {
					log.Errorf(ctx, "[Alloc] plugin %v failed to compute alloc args, request %v, node %v, deploy count %v, err %v", plugin.Name(), resourceOpts, nodeName, deployCount, err)
				}
				return resp, err
			})
			if len(respMap) != len(pm.plugins) {
				return types.ErrAllocFailed
			}

			// calculate engine args
			var err error
			for plugin, resp := range respMap {
				for index, args := range resp.ResourceArgs {
					resResourceArgs[index][plugin.Name()] = args
				}
				for index, args := range resp.EngineArgs {
					resEngineArgs[index], err = pm.mergeEngineArgs(ctx, resEngineArgs[index], args)
					if err != nil {
						log.Errorf(ctx, "[Alloc] invalid engine args")
						return err
					}
				}
			}
			return nil
		},
		// commit: update nodeName resources
		func(ctx context.Context) error {
			if err := pm.UpdateNodeResourceUsage(ctx, nodeName, resResourceArgs, Incr); err != nil {
				log.Errorf(ctx, "[Alloc] failed to update nodeName resource, err: %v", err)
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
func (pm *PluginManager) Realloc(ctx context.Context, nodeName string, originResourceArgs map[string]types.WorkloadResourceArgs, resourceOpts types.WorkloadResourceOpts) (types.EngineArgs, map[string]types.WorkloadResourceArgs, error) {
	resEngineArgs := types.EngineArgs{}
	resDeltaResourceArgs := map[string]types.WorkloadResourceArgs{}
	resFinalResourceArgs := map[string]types.WorkloadResourceArgs{}
	var err error

	return resEngineArgs, resFinalResourceArgs, utils.Pcr(ctx,
		// prepare: calculate engine args, delta node resource args and final workload resource args
		func(ctx context.Context) error {
			respMap := callPlugins(pm.plugins, func(plugin Plugin) (*ReallocResponse, error) {
				resp, err := plugin.Realloc(ctx, nodeName, originResourceArgs[plugin.Name()], resourceOpts)
				if err != nil {
					log.Errorf(ctx, "[Realloc] plugin %v failed to calculate realloc args, err: %v", plugin.Name(), err)
				}
				return resp, err
			})

			if len(respMap) != len(pm.plugins) {
				log.Errorf(ctx, "[Realloc] realloc failed, origin: %+v, opts: %+v", originResourceArgs, resourceOpts)
				return types.ErrReallocFailed
			}

			for plugin, resp := range respMap {
				if resEngineArgs, err = pm.mergeEngineArgs(ctx, resEngineArgs, resp.EngineArgs); err != nil {
					log.Errorf(ctx, "[Realloc] invalid engine args, err: %v", err)
					return types.ErrReallocFailed
				}
				resDeltaResourceArgs[plugin.Name()] = resp.Delta
				resFinalResourceArgs[plugin.Name()] = resp.ResourceArgs
			}
			return nil
		},
		// commit: update node resource
		func(ctx context.Context) error {
			if err := pm.UpdateNodeResourceUsage(ctx, nodeName, []map[string]types.WorkloadResourceArgs{resDeltaResourceArgs}, Incr); err != nil {
				log.Errorf(ctx, "[Alloc] failed to update nodeName resource, err: %v", err)
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

// GetNodeResourceInfo .
func (pm *PluginManager) GetNodeResourceInfo(ctx context.Context, nodeName string, workloads []*types.Workload, fix bool) (map[string]types.NodeResourceArgs, map[string]types.NodeResourceArgs, []string, error) {
	resResourceCapacity := map[string]types.NodeResourceArgs{}
	resResourceUsage := map[string]types.NodeResourceArgs{}
	resDiffs := []string{}

	respMap := callPlugins(pm.plugins, func(plugin Plugin) (*GetNodeResourceInfoResponse, error) {
		resp, err := plugin.GetNodeResourceInfo(ctx, nodeName, workloads, fix)
		if err != nil {
			log.Errorf(ctx, "[GetNodeResourceInfo] plugin %v failed to get node resource of node %v, err: %v", plugin.Name(), nodeName, err)
		}
		return resp, err
	})

	if len(respMap) != len(pm.plugins) {
		return nil, nil, nil, types.ErrGetNodeResourceFailed
	}

	for plugin, resp := range respMap {
		resResourceCapacity[plugin.Name()] = resp.ResourceInfo.Capacity
		resResourceUsage[plugin.Name()] = resp.ResourceInfo.Usage
		resDiffs = append(resDiffs, resp.Diffs...)
	}

	return resResourceCapacity, resResourceUsage, resDiffs, nil
}

// UpdateNodeResourceUsage with rollback
func (pm *PluginManager) UpdateNodeResourceUsage(ctx context.Context, nodeName string, resourceArgs []map[string]types.WorkloadResourceArgs, incr bool) error {
	resourceArgsMap := map[string][]types.WorkloadResourceArgs{}
	rollbackPlugins := []Plugin{}

	return utils.Pcr(ctx,
		// prepare: convert []map[plugin]resourceArgs to map[plugin][]resourceArgs
		// [{"cpu-plugin": {"cpu": 1}}, {"cpu-plugin": {"cpu": 1}}] -> {"cpu-plugin": [{"cpu": 1}, {"cpu": 1}]}
		func(ctx context.Context) error {
			for _, workloadResourceArgs := range resourceArgs {
				for plugin, rawParams := range workloadResourceArgs {
					if _, ok := resourceArgsMap[plugin]; !ok {
						resourceArgsMap[plugin] = []types.WorkloadResourceArgs{}
					}
					resourceArgsMap[plugin] = append(resourceArgsMap[plugin], rawParams)
				}
			}
			return nil
		},
		// commit: call plugins to update node resource
		func(ctx context.Context) error {
			respMap := callPlugins(pm.plugins, func(plugin Plugin) (*UpdateNodeResourceUsageResponse, error) {
				resourceArgs, ok := resourceArgsMap[plugin.Name()]
				if !ok {
					return nil, nil
				}

				resp, err := plugin.UpdateNodeResourceUsage(ctx, nodeName, resourceArgs, incr)
				if err != nil {
					log.Errorf(ctx, "[UpdateNodeResourceUsage] node %v plugin %v failed to update node resource %v, err: %v", nodeName, plugin.Name(), resourceArgs, err)
				}
				return resp, err
			})

			if len(respMap) != len(pm.plugins) {
				for plugin := range respMap {
					rollbackPlugins = append(rollbackPlugins, plugin)
				}

				log.Errorf(ctx, "[UpdateNodeResourceUsage] failed to update node resource for node %v", nodeName)
				return types.ErrUpdateNodeResourceUsageFailed
			}
			return nil
		},
		// rollback: update the rollback resource args in reverse
		func(ctx context.Context) error {
			respMap := callPlugins(rollbackPlugins, func(plugin Plugin) (*UpdateNodeResourceUsageResponse, error) {
				resourceArgs := resourceArgsMap[plugin.Name()]
				resp, err := plugin.UpdateNodeResourceUsage(ctx, nodeName, resourceArgs, !incr)
				if err != nil {
					log.Errorf(ctx, "[UpdateNodeResourceUsage] node %v plugin %v failed to rollback node resource, resourceArgs: %+v, err: %v", resourceArgs, err)
				}
				return resp, err
			})

			if len(respMap) != len(rollbackPlugins) {
				return types.ErrRollbackFailed
			}
			return nil
		},
		pm.config.GlobalTimeout,
	)
}

// UpdateNodeResourceCapacity updates node resource capacity
// receives resource options instead of resource args
func (pm *PluginManager) UpdateNodeResourceCapacity(ctx context.Context, nodeName string, resourceOpts types.NodeResourceOpts, incr bool) error {
	rollbackPlugins := []Plugin{}

	return utils.Pcr(ctx,
		// prepare: do nothing
		func(ctx context.Context) error {
			return nil
		},
		// commit: call plugins to update node resource capacity
		func(ctx context.Context) error {
			respMap := callPlugins(pm.plugins, func(plugin Plugin) (*UpdateNodeResourceCapacityResponse, error) {
				resp, err := plugin.UpdateNodeResourceCapacity(ctx, nodeName, resourceOpts, incr)
				if err != nil {
					log.Errorf(ctx, "[UpdateNodeResourceCapacity] node %v plugin %v failed to update node resource capacity, opts %+v, err %v", nodeName, plugin.Name(), resourceOpts, err)
				}
				return resp, err
			})

			if len(respMap) != len(pm.plugins) {
				for plugin := range respMap {
					rollbackPlugins = append(rollbackPlugins, plugin)
				}

				log.Errorf(ctx, "[UpdateNodeResourceCapacity] node %v opts %+v failed to update node resource capacity, rollback", nodeName, resourceOpts)
				return types.ErrUpdateNodeResourceCapacityFailed
			}
			return nil
		},
		// rollback: update node resource capacity in reverse
		func(ctx context.Context) error {
			respMap := callPlugins(rollbackPlugins, func(plugin Plugin) (*UpdateNodeResourceCapacityResponse, error) {
				resp, err := plugin.UpdateNodeResourceCapacity(ctx, nodeName, resourceOpts, incr)
				if err != nil {
					log.Errorf(ctx, "[UpdateNodeResourceCapacity] node %v plugin %v failed to rollback node resource capacity, opts %+v, err %v", nodeName, plugin.Name(), resourceOpts, err)
				}
				return resp, err
			})
			if len(respMap) != len(rollbackPlugins) {
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
func (pm *PluginManager) Remap(ctx context.Context, nodeName string, workloadMap map[string]*types.Workload) (map[string]types.EngineArgs, error) {
	resEngineArgsMap := map[string]types.EngineArgs{}

	// call plugins to remap
	respMap := callPlugins(pm.plugins, func(plugin Plugin) (*RemapResponse, error) {
		resp, err := plugin.Remap(ctx, nodeName, workloadMap)
		if err != nil {
			log.Errorf(ctx, "[Remap] plugin %v node %v failed to remap, err: %v", plugin.Name(), nodeName, err)
		}
		return resp, err
	})

	if len(respMap) != len(pm.plugins) {
		return nil, types.ErrRemapFailed
	}

	// merge engine args
	var err error
	for _, resp := range respMap {
		for workloadID, engineArgs := range resp.EngineArgsMap {
			if _, ok := resEngineArgsMap[workloadID]; !ok {
				resEngineArgsMap[workloadID] = types.EngineArgs{}
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
func (pm *PluginManager) AddNode(ctx context.Context, nodeName string, resourceOpts types.NodeResourceOpts) (map[string]types.NodeResourceArgs, map[string]types.NodeResourceArgs, error) {
	resResourceCapacity := map[string]types.NodeResourceArgs{}
	resResourceUsage := map[string]types.NodeResourceArgs{}
	rollbackPlugins := []Plugin{}

	return resResourceCapacity, resResourceUsage, utils.Pcr(ctx,
		// prepare: do nothing
		func(ctx context.Context) error {
			return nil
		},
		// commit: call plugins to add the node
		func(ctx context.Context) error {
			respMap := callPlugins(pm.plugins, func(plugin Plugin) (*AddNodeResponse, error) {
				resp, err := plugin.AddNode(ctx, nodeName, resourceOpts)
				if err != nil {
					log.Errorf(ctx, "[AddNode] node %v plugin %v failed to add node, req: %v, err: %v", nodeName, plugin.Name(), resourceOpts, err)
				}
				return resp, err
			})

			if len(respMap) != len(pm.plugins) {
				for plugin := range respMap {
					rollbackPlugins = append(rollbackPlugins, plugin)
				}

				log.Errorf(ctx, "[AddNode] node %v failed to add node %v, rollback", nodeName, resourceOpts)
				return types.ErrAddNodeFailed
			}

			for plugin, resp := range respMap {
				resResourceCapacity[plugin.Name()] = resp.Capacity
				resResourceUsage[plugin.Name()] = resp.Usage
			}

			return nil
		},
		// rollback: remove node
		func(ctx context.Context) error {
			respMap := callPlugins(rollbackPlugins, func(plugin Plugin) (*RemoveNodeResponse, error) {
				resp, err := plugin.RemoveNode(ctx, nodeName)
				if err != nil {
					log.Errorf(ctx, "[AddNode] node %v plugin %v failed to rollback, err: %v", nodeName, plugin.Name(), err)
				}
				return resp, err
			})

			if len(respMap) != len(rollbackPlugins) {
				log.Errorf(ctx, "[AddNode] failed to rollback")
				return types.ErrRollbackFailed
			}

			return nil
		},
		pm.config.GlobalTimeout,
	)
}

// RemoveNode .
func (pm *PluginManager) RemoveNode(ctx context.Context, nodeName string) error {
	var resourceCapacityMap map[string]types.NodeResourceArgs
	var resourceUsageMap map[string]types.NodeResourceArgs
	rollbackPlugins := []Plugin{}

	return utils.Pcr(ctx,
		// prepare: get node resource
		func(ctx context.Context) error {
			var err error
			resourceCapacityMap, resourceUsageMap, _, err = pm.GetNodeResourceInfo(ctx, nodeName, nil, false)
			if err != nil {
				log.Errorf(ctx, "[RemoveNode] failed to get node %v resource, err: %v", nodeName, err)
				return err
			}
			return nil
		},
		// commit: remove node
		func(ctx context.Context) error {
			respMap := callPlugins(pm.plugins, func(plugin Plugin) (*RemoveNodeResponse, error) {
				resp, err := plugin.RemoveNode(ctx, nodeName)
				if err != nil {
					log.Errorf(ctx, "[AddNode] plugin %v failed to remove node, err: %v", plugin.Name(), nodeName, err)
				}
				return resp, err
			})

			if len(respMap) != len(pm.plugins) {
				for plugin := range respMap {
					rollbackPlugins = append(rollbackPlugins, plugin)
				}

				log.Errorf(ctx, "[AddNode] failed to remove node %v", nodeName)
				return types.ErrRemoveNodeFailed
			}
			return nil
		},
		// rollback: add node
		func(ctx context.Context) error {
			respMap := callPlugins(rollbackPlugins, func(plugin Plugin) (*SetNodeResourceInfoResponse, error) {
				resp, err := plugin.SetNodeResourceInfo(ctx, nodeName, resourceCapacityMap[plugin.Name()], resourceUsageMap[plugin.Name()])
				if err != nil {
					log.Errorf(ctx, "[RemoveNode] plugin %v node %v failed to rollback, err: %v", plugin.Name(), nodeName, err)
				}
				return resp, err
			})

			if len(respMap) != len(rollbackPlugins) {
				log.Errorf(ctx, "[RemoveNode] failed to rollback")
				return types.ErrRollbackFailed
			}
			return nil
		},
		pm.config.GlobalTimeout,
	)
}
