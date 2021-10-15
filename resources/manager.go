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

// GetAvailableNodes returns available nodes which meet all the requirements
// the caller should require locks
func (pm *PluginManager) GetAvailableNodes(ctx context.Context, rawRequests RawParams) (map[string]*resourcetypes.NodeResourceInfo, int, error) {
	res := map[string]*resourcetypes.NodeResourceInfo{}
	errChan := make(chan error, len(pm.plugins))
	infoChan := make(chan map[string]*resourcetypes.NodeResourceInfo, len(pm.plugins))

	pm.callPlugins(func(plugin Plugin) {
		info, _, err := plugin.GetAvailableNodes(ctx, rawRequests)
		if err != nil {
			log.Errorf(ctx, "[GetAvailableNodes] plugin %v failed to get available nodes, request %v, err %v", plugin.Name(), rawRequests, err)
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

// mergeEngineArgs e.g. {"file": ["f1", "f2"]} + {"file": ["f3"], "cpu": ["1"]} => {"file": ["f1", "f2", "f3"], "cpu": ["1"]}
func (pm *PluginManager) mergeEngineArgs(m1 RawParams, m2 RawParams) RawParams {
	res := RawParams{}
	for key, value := range m1 {
		res[key] = value
	}
	for key, value := range m2 {
		if _, ok := res[key]; ok {
			res[key] = append(res[key], value...)
		} else {
			res[key] = value
		}
	}
	return res
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

	for engineArgs := range engineArgsChan {
		for index, params := range engineArgs {
			resEngineArgs[index] = pm.mergeEngineArgs(resEngineArgs[index], params)
		}
	}

	return resEngineArgs, resResourceArgs, nil
}

// GetNodesResource .
func (pm *PluginManager) GetNodesResource(ctx context.Context, nodes []string) (map[string]map[string]string, error) {
	panic("implement me")
}

// Rollback .
func (pm *PluginManager) Rollback(ctx context.Context, node string, resourceArgs []map[string]RawParams) error {
	// convert []map[plugin]resourceArgs to map[plugin][]resourceArgs
	// [{"cpu-plugin": {"cpu": ["1"]}}, {"cpu-plugin": {"cpu": ["1"]}}] -> {"cpu-plugin": [{"cpu": ["1"]}, {"cpu": ["1"]}]}
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

	// call plugins to rollback
	pm.callPlugins(func(plugin Plugin) {
		rollbackArgs, ok := rollbackArgsMap[plugin.Name()]
		if !ok {
			return
		}

		if err := plugin.Rollback(ctx, node, rollbackArgs); err != nil {
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
	for engineArgsMap := range engineArgsMapChan {
		for workloadID, engineArgs := range engineArgsMap {
			if _, ok := resEngineArgsMap[workloadID]; !ok {
				resEngineArgsMap[workloadID] = RawParams{}
			}
			resEngineArgsMap[workloadID] = pm.mergeEngineArgs(resEngineArgsMap[workloadID], engineArgs)
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
