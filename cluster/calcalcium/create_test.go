package calcalcium

import (
	"context"
	"testing"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/resources"
	"github.com/projecteru2/core/resources/mocks"
	storemocks "github.com/projecteru2/core/store/mocks"
	"github.com/projecteru2/core/strategy"
	"github.com/projecteru2/core/types"

	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/assert"
)

func TestCreateWorkloadWithResourcePlugin(t *testing.T) {
	c := NewTestCluster()
	c.store = storemocks.FromTemplate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.resource = resources.NewPluginManager(ctx, c.config)
	c.resource.AddPlugins(mocks.NewMockCpuPlugin(), mocks.NewMockMemPlugin())
	opts := &types.DeployOptions{
		Name:    "deployname",
		Podname: "somepod",
		Image:   "image:todeploy",
		Count:   2,
		Entrypoint: &types.Entrypoint{
			Name: "some-nice-entrypoint",
		},
		ResourceOpts: map[string][]string{
			"cpu": {"1"},
			"mem": {"100000PB"},
		},
		DeployStrategy: strategy.Auto,
	}

	ch, err := c.CreateWorkload(ctx, opts)
	assert.Nil(t, err)
	for msg := range ch {
		log.Infof(ctx, "create workload msg: %+v", litter.Sdump(msg))
	}
}
