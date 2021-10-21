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
	if len(pm.config.ResourcePluginDir) > 0 {
		pluginFiles, err := utils.ListAllExecutableFiles(pm.config.ResourcePluginDir)
		if err != nil {
			log.Errorf(ctx, "[LoadPlugins] failed to list all executable files dir: %v, err: %v", pm.config.ResourcePluginDir, err)
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

func (pm *PluginManager) callPlugins(f func(Plugin)) {
	wg := &sync.WaitGroup{}
	wg.Add(len(pm.plugins))

	for _, plugin := range pm.plugins {
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

	// try to call all plugins to lock nodes
	pm.callPlugins(func(plugin Plugin) {
		if err := plugin.LockNodes(ctx, nodes); err != nil {
			log.Errorf(ctx, "[LockNodes] plugin %v failed to lock nodes %v, err: %v", plugin.Name(), nodes, err)
		} else {
			locked <- plugin
		}
	})
	close(locked)

	if len(locked) != len(pm.plugins) {
		log.Errorf(ctx, "[LockNodes] failed to lock nodes %v, rollback", nodes)
		// rollback
		pm.callPlugins(func(plugin Plugin) {
			if err := plugin.UnlockNodes(ctx, nodes); err != nil {
				log.Errorf(ctx, "[LockNodes] plugin %v failed to unlock nodes %v, err: %v", plugin.Name(), nodes, err)
			}
		})
		return types.ErrLockFailed
	}
	log.Infof(ctx, "[LockNodes] locked nodes %v on all plugins", nodes)
	return nil
}

// UnlockNodes .
func (pm *PluginManager) UnlockNodes(ctx context.Context, nodes []string) error {
	unlocked := make(chan Plugin, len(pm.plugins))

	// try to call all plugins to lock nodes
	pm.callPlugins(func(plugin Plugin) {
		if err := plugin.LockNodes(ctx, nodes); err != nil {
			log.Errorf(ctx, "[UnlockNodes] plugin %v failed to lock nodes %v, err: %v", plugin.Name(), nodes, err)
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
}

// WithNodesLocked .
func (pm *PluginManager) WithNodesLocked(ctx context.Context, nodes []string, f func(context.Context) error) error {
	if err := pm.LockNodes(ctx, nodes); err != nil {
		return err
	}
	defer pm.UnlockNodes(ctx, nodes)

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
				Count:    info1.Count + 1,
			}
		}
	}
	return res
}

// SelectAvailableNodes returns available nodes which meet all the requirements
// the caller should require locks
func (pm *PluginManager) SelectAvailableNodes(ctx context.Context, nodes []string, rawRequests RawParams) (map[string]*resourcetypes.NodeResourceInfo, int, error) {
	res := map[string]*resourcetypes.NodeResourceInfo{}
	errChan := make(chan error, len(pm.plugins))
	infoChan := make(chan map[string]*resourcetypes.NodeResourceInfo, len(pm.plugins))

	pm.callPlugins(func(plugin Plugin) {
		info, _, err := plugin.SelectAvailableNodes(ctx, nil, rawRequests)
		if err != nil {
			log.Errorf(ctx, "[SelectAvailableNodes] plugin %v failed to get available nodes, request %v, err %v", plugin.Name(), rawRequests, err)
			errChan <- err
		} else {
			infoChan <- info
		}
	})
	close(errChan)
	close(infoChan)

	if len(errChan) > 0 {
		return nil, 0, types.ErrGetAvailableNodesFailed
	}

	// get nodes with all resource capacities > 0
	for info := range infoChan {
		res = pm.mergeNodeResourceInfo(res, info)
	}

	total := 0

	// weighted average
	for _, info := range res {
		info.Rate /= float64(info.Count + 1)
		info.Weight /= float64(info.Count + 1)
		total += info.Capacity
	}

	return res, total, nil
}

// mergeEngineArgs e.g. {"file": ["/bin/sh:/bin/sh"], "cpu": 1.2, "cpu-bind": true} + {"file": ["/bin/ls:/bin/ls"], "mem": "1PB"}
// => {"file": ["/bin/sh:/bin/sh", "/bin/ls:/bin/ls"], "cpu": 1.2, "cpu-bind": true, "mem": "1PB"}
func (pm *PluginManager) mergeEngineArgs(ctx context.Context, m1 RawParams, m2 RawParams) (RawParams, error) {
	res := RawParams{}
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
func (pm *PluginManager) Alloc(ctx context.Context, node string, deployCount int, rawRequest RawParams) ([]RawParams, []map[string]RawParams, error) {
	// engine args for each workload. format: [{"file": ["/bin/sh:/bin/sh"]}]
	resEngineArgs := make([]RawParams, deployCount)
	// resource args for each plugin and each workload. format: [{"cpu-plugin": {"cpu": ["1"]}}]
	resResourceArgs := make([]map[string]RawParams, deployCount)

	// init engine args
	for i := 0; i < deployCount; i++ {
		resEngineArgs[i] = RawParams{}
		resResourceArgs[i] = map[string]RawParams{}
	}

	engineArgsChan := make(chan []RawParams, len(pm.plugins))
	errChan := make(chan error, len(pm.plugins))

	// call all plugins to alloc resource
	pm.callPlugins(func(plugin Plugin) {
		engineArgs, resourceArgs, err := plugin.Alloc(ctx, node, deployCount, rawRequest)
		if err != nil {
			log.Errorf(ctx, "[Alloc] plugin %v failed to alloc, request %v, node %v, deploy count %v, err %v", plugin.Name(), rawRequest, node, deployCount, err)
			errChan <- err
		} else {
			engineArgsChan <- engineArgs
			for index, resourceArgsMap := range resResourceArgs {
				resourceArgsMap[plugin.Name()] = resourceArgs[index]
			}
		}
	})
	close(errChan)
	close(engineArgsChan)

	if len(errChan) > 0 {
		return nil, nil, types.ErrAllocFailed
	}

	var err error
	for engineArgs := range engineArgsChan {
		for index, args := range engineArgs {
			resEngineArgs[index], err = pm.mergeEngineArgs(ctx, resEngineArgs[index], args)
			if err != nil {
				log.Errorf(ctx, "[Alloc] invalid engine args")
				return nil, nil, err
			}
		}
	}

	return resEngineArgs, resResourceArgs, nil
}

// Realloc reallocates resource for workloads, returns engine args and resource args for each workload.
// format of engine args: {"workload1": {"cpu": 1.2}}
// format of resource args: {"workload1": {"cpu-plugin": {"cpu": 1.2}}}
func (pm *PluginManager) Realloc(ctx context.Context, workloads []*types.Workload, resourceOpts RawParams) (map[string]RawParams, map[string]map[string]RawParams, error) {
	engineArgsMap := map[string]RawParams{}
	resourceArgsMap := map[string]map[string]RawParams{}
	workloadIDs := []string{}

	for _, workload := range workloads {
		engineArgsMap[workload.ID] = RawParams{}
		resourceArgsMap[workload.ID] = map[string]RawParams{}
		workloadIDs = append(workloadIDs, workload.ID)
	}

	errChan := make(chan error, len(pm.plugins))

	pm.callPlugins(func(plugin Plugin) {
		engineArgs, resourceArgs, err := plugin.Realloc(ctx, workloads, resourceOpts)
		if err != nil {
			log.Errorf(ctx, "[Realloc] plugin %v failed to realloc for workloads %v, err: %v", plugin.Name(), workloadIDs, err)
			errChan <- err
		} else {
			for workloadID, args := range engineArgs {
				engineArgsMap[workloadID], err = pm.mergeEngineArgs(ctx, engineArgsMap[workloadID], args)
				if err != nil {
					log.Errorf(ctx, "[Realloc] plugin %v failed to merge engine args for workload %v, err: %v", plugin.Name(), workloadID, err)
					errChan <- err
					return
				}
				resourceArgsMap[workloadID] = resourceArgs
			}
		}
	})

	close(errChan)
	if len(errChan) > 0 {
		return nil, nil, types.ErrReallocFailed
	}

	return engineArgsMap, resourceArgsMap, nil
}

// GetNodeResource .
func (pm *PluginManager) GetNodeResource(ctx context.Context, node string, workloads []*types.Workload, fix bool) (map[string]RawParams, []string, error) {
	resNodeResource := map[string]RawParams{}
	resDiffs := []string{}

	diffChan := make(chan []string, len(pm.plugins))
	errChan := make(chan error, len(pm.plugins))

	pm.callPlugins(func(plugin Plugin) {
		nodeResource, diffs, err := plugin.GetNodeResource(ctx, node, workloads, fix)
		if err != nil {
			log.Errorf(ctx, "[GetNodeResource] plugin %v failed to get node resource of node %v, err: %v", plugin.Name(), node, err)
			errChan <- err
		} else {
			diffChan <- diffs
			resNodeResource[plugin.Name()] = nodeResource
		}
	})

	close(errChan)
	close(diffChan)

	if len(errChan) > 0 {
		return nil, nil, types.ErrGetNodeResourceFailed
	}

	for diffs := range diffChan {
		resDiffs = append(resDiffs, diffs...)
	}

	return resNodeResource, resDiffs, nil
}

// UpdateNodeResource .
func (pm *PluginManager) UpdateNodeResource(ctx context.Context, node string, resourceArgs []map[string]RawParams, direction bool) error {
	// convert []map[plugin]resourceArgs to map[plugin][]resourceArgs
	// [{"cpu-plugin": {"cpu": 1}}, {"cpu-plugin": {"cpu": 1}}] -> {"cpu-plugin": [{"cpu": 1}, {"cpu": 1}]}
	rollbackArgsMap := map[string][]RawParams{}
	for _, workloadResourceArgs := range resourceArgs {
		for plugin, rawParams := range workloadResourceArgs {
			if _, ok := rollbackArgsMap[plugin]; !ok {
				rollbackArgsMap[plugin] = []RawParams{}
			}
			rollbackArgsMap[plugin] = append(rollbackArgsMap[plugin], rawParams)
		}
	}

	errChan := make(chan error, len(pm.plugins))

	// call plugins to return resources
	pm.callPlugins(func(plugin Plugin) {
		rollbackArgs, ok := rollbackArgsMap[plugin.Name()]
		if !ok {
			return
		}

		if err := plugin.UpdateNodeResource(ctx, node, rollbackArgs, direction); err != nil {
			log.Errorf(ctx, "[Rollback] node %v plugin %v failed to rollback %v, err: %v", node, plugin.Name(), rollbackArgs, err)
			errChan <- err
		}
	})

	close(errChan)
	if len(errChan) > 0 {
		log.Errorf(ctx, "[Rollback] node %v failed to rollback %v", node, resourceArgs)
		return types.ErrRollbackFailed
	}

	return nil
}

// Remap remaps resource and returns engine args for workloads. format: {"workload-1": {"cpus": ["1-3"]}}
func (pm *PluginManager) Remap(ctx context.Context, node string, workloadMap map[string]*types.Workload) (map[string]RawParams, error) {
	resEngineArgsMap := map[string]RawParams{}

	engineArgsMapChan := make(chan map[string]RawParams, len(pm.plugins))
	errChan := make(chan error, len(pm.plugins))

	// call plugins to remap
	pm.callPlugins(func(plugin Plugin) {
		engineArgsMap, err := plugin.Remap(ctx, node, workloadMap)
		if err != nil {
			log.Errorf(ctx, "[Remap] plugin %v node %v failed to remap, err: %v", plugin.Name(), node, err)
			errChan <- err
		} else {
			engineArgsMapChan <- engineArgsMap
		}
	})
	close(errChan)
	close(engineArgsMapChan)

	if len(errChan) > 0 {
		return nil, types.ErrRemapFailed
	}

	// merge engine args
	var err error
	for engineArgsMap := range engineArgsMapChan {
		for workloadID, engineArgs := range engineArgsMap {
			if _, ok := resEngineArgsMap[workloadID]; !ok {
				resEngineArgsMap[workloadID] = RawParams{}
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

// SetNodeResource .
func (pm *PluginManager) SetNodeResource(ctx context.Context, node string, rawRequest RawParams) error {
	errChan := make(chan error, len(pm.plugins))

	// call plugins to rollback
	pm.callPlugins(func(plugin Plugin) {
		if err := plugin.SetNodeResource(ctx, node, rawRequest); err != nil {
			log.Errorf(ctx, "[SetNodeResource] node %v plugin %v failed to set node resource %v, err: %v", node, plugin.Name(), rawRequest, err)
			errChan <- err
		}
	})

	close(errChan)
	if len(errChan) > 0 {
		log.Errorf(ctx, "[SetNodeResource] node %v failed to set node resource %v", node, rawRequest)
		return types.ErrSetNodeResourceFailed
	}

	return nil
}

// RemoveNode .
func (pm *PluginManager) RemoveNode(ctx context.Context, node string) error {
	errChan := make(chan error, len(pm.plugins))

	// call plugins to rollback
	pm.callPlugins(func(plugin Plugin) {
		if err := plugin.RemoveNode(ctx, node); err != nil {
			log.Errorf(ctx, "[SetNodeResource] plugin %v failed to remove node, err: %v", plugin.Name(), node, err)
			errChan <- err
		}
	})

	close(errChan)
	if len(errChan) > 0 {
		log.Errorf(ctx, "[SetNodeResource] failed to remove node %v", node)
		return types.ErrRemoveNodeFailed
	}

	return nil
}
