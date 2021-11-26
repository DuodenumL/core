package mocks

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/mock"

	"github.com/projecteru2/core/engine"
	enginemocks "github.com/projecteru2/core/engine/mocks"
	types2 "github.com/projecteru2/core/engine/types"
	"github.com/projecteru2/core/lock"
	lockmocks "github.com/projecteru2/core/lock/mocks"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/store"
	"github.com/projecteru2/core/types"
	"github.com/projecteru2/core/utils"
)

type MockStore struct {
	Store
	workloads *sync.Map
	nodes     *sync.Map
	locks     *sync.Map
}

var startVirtualizationCounter = 0

func getEngine() engine.API {
	m := &enginemocks.API{}
	m.On("VirtualizationCreate", mock.Anything, mock.Anything).Return(func(ctx context.Context, config *types2.VirtualizationCreateOptions) *types2.VirtualizationCreated {
		id := utils.RandomString(16)
		log.Infof(ctx, "create virtualization %v, config %+v", id, config)
		return &types2.VirtualizationCreated{
			ID: id,
		}
	}, nil)
	m.On("VirtualizationInspect", mock.Anything, mock.Anything).Return(func(ctx context.Context, id string) *types2.VirtualizationInfo {
		return &types2.VirtualizationInfo{
			ID:      id,
			Running: true,
		}
	}, nil)
	m.On("VirtualizationStart", mock.Anything, mock.Anything).Return(func(ctx context.Context, id string) error {
		success := startVirtualizationCounter != 1
		startVirtualizationCounter++
		log.Infof(ctx, "start virtualization: %v", success)
		if !success {
			return errors.New("random failure")
		}
		return nil
	})
	m.On("VirtualizationStop", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, ID string, _ time.Duration) error {
		log.Infof(ctx, "stop virtualization %v", ID)
		return nil
	})
	m.On("VirtualizationRemove", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, ID string, volumes bool, force bool) error {
		log.Infof(ctx, "remove virtualization %v", ID)
		return nil
	})
	m.On("VirtualizationUpdateResource", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, ID string, resource *types2.VirtualizationResource) error {
		log.Infof(ctx, "update virtualization %v %v", ID, litter.Sdump(resource))
		return nil
	})
	m.On("ImageLocalDigests", mock.Anything, mock.Anything).Return([]string{"dig"}, nil)
	m.On("ImageRemoteDigest", mock.Anything, mock.Anything).Return("dig", nil)

	return m
}

func getNodes() *sync.Map {
	m := &sync.Map{}
	m.Store("node1", &types.Node{
		NodeMeta: types.NodeMeta{
			Name: "node1",
		},
		Available: true,
		Engine:    getEngine(),
	})
	m.Store("node2", &types.Node{
		NodeMeta: types.NodeMeta{
			Name: "node2",
		},
		Available: true,
		Engine:    getEngine(),
	})
	m.Store("node3", &types.Node{
		NodeMeta: types.NodeMeta{
			Name: "node3",
		},
		Available: true,
		Engine:    getEngine(),
	})
	return m
}

func FromTemplate() store.Store {
	m := &MockStore{}
	m.workloads = &sync.Map{}
	m.nodes = getNodes()
	m.locks = &sync.Map{}

	m.On("GetNodesByPod", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(_ context.Context, _ string, _ map[string]string, _ bool) []*types.Node {
		res := []*types.Node{}
		m.nodes.Range(func(key, value interface{}) bool {
			res = append(res, value.(*types.Node))
			return true
		})
		return res
	}, nil)
	m.On("GetNode", mock.Anything, mock.Anything).Return(func(_ context.Context, nodename string) *types.Node {
		node, _ := m.nodes.Load(nodename)
		return node.(*types.Node)
	}, nil)
	m.On("AddWorkload", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, workload *types.Workload, _ *types.Processing) error {
		log.Infof(ctx, "add workload %+v", workload)
		m.workloads.Store(workload.ID, workload)
		return nil
	})
	m.On("UpdateWorkload", mock.Anything, mock.Anything).Return(func(ctx context.Context, workload *types.Workload) error {
		log.Infof(ctx, "update workload %+v", workload)
		m.workloads.Store(workload.ID, workload)
		return nil
	})
	m.On("GetWorkload", mock.Anything, mock.Anything).Return(func(ctx context.Context, id string) *types.Workload {
		res, _ := m.workloads.Load(id)
		return res.(*types.Workload)
	}, nil)
	m.On("ListWorkloads", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, appname string, entrypoint string, nodename string, limit int64, labels map[string]string) []*types.Workload {
		res := []*types.Workload{}
		m.workloads.Range(func(_, v interface{}) bool {
			res = append(res, v.(*types.Workload))
			return true
		})
		return res
	}, nil)
	m.On("GetWorkloads", mock.Anything, mock.Anything).Return(func(ctx context.Context, ids []string) []*types.Workload {
		res := []*types.Workload{}
		for _, id := range ids {
			if v, ok := m.workloads.Load(id); ok {
				res = append(res, v.(*types.Workload))
			}
		}
		return res
	}, nil)
	m.On("RemoveWorkload", mock.Anything, mock.Anything).Return(func(ctx context.Context, workload *types.Workload) error {
		log.Infof(ctx, "delete workload %v", workload.ID)
		m.workloads.Delete(workload.ID)
		return nil
	})
	m.On("GetDeployStatus", mock.Anything, mock.Anything, mock.Anything).Return(func(_ context.Context, appname, entryname string) map[string]int {
		res := map[string]int{}
		m.workloads.Range(func(_, workload interface{}) bool {
			res[workload.(*types.Workload).Nodename]++
			return true
		})
		return res
	}, nil)
	m.On("CreateProcessing", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	m.On("DeleteProcessing", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	m.On("CreateLock", mock.Anything, mock.Anything).Return(func(_ string, _ time.Duration) lock.DistributedLock { return lockmocks.FromTemplate() }, nil)
	m.On("ListNodeWorkloads", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, node string, labels map[string]string) []*types.Workload {
		res := []*types.Workload{}
		m.workloads.Range(func(_, workload interface{}) bool {
			res = append(res, workload.(*types.Workload))
			return true
		})
		return res
	}, nil)
	m.On("ListPodNodes", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, podname string, lables map[string]string, all bool) []*types.Node {
		res := []*types.Node{}
		m.nodes.Range(func(_, node interface{}) bool {
			res = append(res, node.(*types.Node))
			return true
		})
		return res
	}, nil)

	return m
}
