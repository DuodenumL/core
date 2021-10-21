package calcium

import (
	"context"
	"testing"

	enginemocks "github.com/projecteru2/core/engine/mocks"
	enginetypes "github.com/projecteru2/core/engine/types"
	lockmocks "github.com/projecteru2/core/lock/mocks"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources"
	"github.com/projecteru2/core/resources/mocks"
	storemocks "github.com/projecteru2/core/store/mocks"
	"github.com/projecteru2/core/strategy"
	"github.com/projecteru2/core/types"

	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func createMockWorkloadWithResourcePlugin(t *testing.T, ctx context.Context, c *Calcium) []string {
	createOpts := &types.DeployOptions{
		Name:    "deployname",
		Podname: "somepod",
		Image:   "image:todeploy",
		Count:   2,
		Entrypoint: &types.Entrypoint{
			Name: "some-nice-entrypoint",
		},
		ResourceOpts: map[string]interface{}{
			"cpu": 1.2,
			"mem": "100000PB",
		},
		DeployStrategy: strategy.Auto,
	}

	ch, err := c.CreateWorkload(ctx, createOpts)
	assert.Nil(t, err)

	ids := []string{}
	for msg := range ch {
		log.Infof(ctx, "create workload %v err %v", msg.WorkloadID, msg.Error)
		if msg.Error == nil {
			ids = append(ids, msg.WorkloadID)
		}
	}

	log.Info("===================create done===============")
	return ids
}

func TestReplaceWorkloadWithResourcePlugin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewTestCluster()
	c.store = storemocks.FromTemplate()
	c.resource = resources.NewPluginManager(ctx, c.config)
	c.resource.AddPlugins(mocks.NewMockCpuPlugin(), mocks.NewMockMemPlugin())

	createMockWorkloadWithResourcePlugin(t, ctx, c)

	opts := &types.ReplaceOptions{
		DeployOptions: types.DeployOptions{
			Name:    "deployname",
			Podname: "somepod",
			Image:   "image:todeploy",
			Entrypoint: &types.Entrypoint{
				Name: "some-nice-entrypoint",
			},
			ResourceOpts: map[string]interface{}{
				"cpu": 1.2,
				"mem": "100000PB",
			},
		},
	}
	replaceMsgCh, err := c.ReplaceWorkload(ctx, opts)
	assert.Nil(t, err)
	for msg := range replaceMsgCh {
		log.Infof(ctx, "replace msg: %v", litter.Sdump(msg))
		assert.Nil(t, msg.Error)
	}
}

func TestReplaceWorkload(t *testing.T) {
	c := NewTestCluster()
	ctx := context.Background()
	lock := &lockmocks.DistributedLock{}
	lock.On("Lock", mock.Anything).Return(context.TODO(), nil)
	lock.On("Unlock", mock.Anything).Return(nil)
	store := c.store.(*storemocks.Store)

	_, err := c.ReplaceWorkload(ctx, &types.ReplaceOptions{
		DeployOptions: types.DeployOptions{
			Entrypoint: &types.Entrypoint{
				Name: "bad_entrypoint_name",
			},
		},
	})
	assert.Error(t, err)

	opts := &types.ReplaceOptions{
		DeployOptions: types.DeployOptions{
			Name:  "appname",
			Image: "image:latest",
			Entrypoint: &types.Entrypoint{
				Name: "nice-entry-name",
			},
		},
	}

	workload := &types.Workload{
		ID:       "xx",
		Name:     "yy",
		Nodename: "testnode",
	}
	// failed by ListWorkload
	store.On("ListWorkloads", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, types.ErrNoETCD).Once()
	_, err = c.ReplaceWorkload(ctx, opts)
	assert.Error(t, err)
	store.AssertExpectations(t)

	store.On("ListWorkloads", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*types.Workload{workload}, nil)
	// failed by withWorkloadLocked
	store.On("GetWorkloads", mock.Anything, mock.Anything).Return(nil, types.ErrNoETCD).Once()
	ch, err := c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.Error(t, r.Error)
	}
	store.AssertExpectations(t)

	store.On("GetWorkloads", mock.Anything, mock.Anything).Return([]*types.Workload{workload}, nil).Once()
	store.On("CreateLock", mock.Anything, mock.Anything).Return(lock, nil)
	// ignore because pod not fit
	opts.Podname = "wtf"
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for range ch {
	}
	store.AssertExpectations(t)

	workload.Podname = "wtf"
	opts.NetworkInherit = true
	store.On("GetWorkloads", mock.Anything, mock.Anything).Return([]*types.Workload{workload}, nil).Once()
	// failed by inspect
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.Error(t, r.Error)
	}
	store.AssertExpectations(t)

	engine := &enginemocks.API{}
	workload.Engine = engine
	store.On("GetWorkloads", mock.Anything, mock.Anything).Return([]*types.Workload{workload}, nil)
	// failed by not running
	engine.On("VirtualizationInspect", mock.Anything, mock.Anything).Return(&enginetypes.VirtualizationInfo{Running: false}, nil).Once()
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.Error(t, r.Error)
	}
	store.AssertExpectations(t)

	engine.On("VirtualizationInspect", mock.Anything, mock.Anything).Return(&enginetypes.VirtualizationInfo{Running: true}, nil)
	// failed by not fit
	opts.FilterLabels = map[string]string{"x": "y"}
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.Error(t, r.Error)
		assert.NotNil(t, r.Remove)
		assert.False(t, r.Remove.Success)
	}
	store.AssertExpectations(t)

	// failed by get node
	opts.FilterLabels = map[string]string{}
	store.On("GetNode", mock.Anything, mock.Anything).Return(nil, types.ErrNoETCD).Once()
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.Error(t, r.Error)
		assert.NotNil(t, r.Remove)
		assert.False(t, r.Remove.Success)
	}
	node := &types.Node{
		NodeMeta: types.NodeMeta{
			Name: "test",
		},
	}
	store.On("GetNode", mock.Anything, mock.Anything).Return(node, nil).Once()
	// failed by no image
	opts.Image = ""
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.Error(t, r.Error)
		assert.NotNil(t, r.Remove)
		assert.False(t, r.Remove.Success)
	}
	opts.Image = "image:latest"
	node.Engine = engine
	engine.On("ImageLocalDigests", mock.Anything, mock.Anything).Return([]string{"id"}, nil)
	engine.On("ImageRemoteDigest", mock.Anything, mock.Anything).Return("id", nil)
	store.On("GetNode", mock.Anything, mock.Anything).Return(node, nil)
	// failed by VirtualizationCopyFrom
	opts.Copy = map[string]string{"src": "dst"}
	engine.On("VirtualizationCopyFrom", mock.Anything, mock.Anything, mock.Anything).Return(nil, 0, 0, int64(0), types.ErrBadWorkloadID).Once()
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.Error(t, r.Error)
		assert.NotNil(t, r.Remove)
		assert.False(t, r.Remove.Success)
	}
	store.AssertExpectations(t)
	engine.AssertExpectations(t)

	engine.On("VirtualizationCopyFrom", mock.Anything, mock.Anything, mock.Anything).Return([]byte{}, 0, 0, int64(0), nil)
	// failed by Stop
	engine.On("VirtualizationStop", mock.Anything, mock.Anything, mock.Anything).Return(types.ErrCannotGetEngine).Once()
	engine.On("VirtualizationStart", mock.Anything, mock.Anything).Return(types.ErrCannotGetEngine).Once()
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.Error(t, r.Error)
		assert.NotNil(t, r.Remove)
		assert.False(t, r.Remove.Success)
	}
	store.AssertExpectations(t)
	engine.AssertExpectations(t)

	engine.On("VirtualizationStop", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// failed by VirtualizationCreate
	engine.On("VirtualizationCreate", mock.Anything, mock.Anything).Return(nil, types.ErrCannotGetEngine).Once()
	engine.On("VirtualizationStart", mock.Anything, mock.Anything).Return(types.ErrCannotGetEngine).Once()
	//store.On("UpdateNodeResource", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	//engine.On("VirtualizationRemove", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	//store.On("RemoveWorkload", mock.Anything, mock.Anything).Return(nil).Once()
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.Error(t, r.Error)
		assert.NotNil(t, r.Remove)
		assert.False(t, r.Remove.Success)
	}
	store.AssertExpectations(t)
	engine.AssertExpectations(t)

	engine.On("VirtualizationCreate", mock.Anything, mock.Anything).Return(&enginetypes.VirtualizationCreated{ID: "new"}, nil)
	engine.On("VirtualizationStart", mock.Anything, mock.Anything).Return(nil)
	engine.On("VirtualizationCopyTo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	engine.On("VirtualizationInspect", mock.Anything, mock.Anything).Return(&enginetypes.VirtualizationInfo{User: "test"}, nil)
	store.On("AddWorkload", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// failed by remove workload
	store.On("RemoveWorkload", mock.Anything, mock.Anything).Return(types.ErrNoETCD).Once()
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.Error(t, r.Error)
		assert.NotNil(t, r.Remove)
		assert.NotNil(t, r.Create)
		assert.False(t, r.Remove.Success)
		assert.Nil(t, r.Create.Error)
	}
	store.AssertExpectations(t)
	engine.AssertExpectations(t)

	engine.On("VirtualizationRemove", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.On("RemoveWorkload", mock.Anything, mock.Anything).Return(nil)
	store.On("ListNodeWorkloads", mock.Anything, mock.Anything, mock.Anything).Return(nil, types.ErrNoETCD)
	// succ
	ch, err = c.ReplaceWorkload(ctx, opts)
	assert.NoError(t, err)
	for r := range ch {
		assert.NoError(t, r.Error)
		assert.NotNil(t, r.Remove)
		assert.NotNil(t, r.Create)
		assert.True(t, r.Remove.Success)
		assert.Nil(t, r.Create.Error)
	}
}
