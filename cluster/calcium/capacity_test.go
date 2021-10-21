package calcium

import (
	"context"
	"fmt"
	"testing"

	"github.com/projecteru2/core/resources"
	"github.com/projecteru2/core/resources/mocks"
	storemocks "github.com/projecteru2/core/store/mocks"
	"github.com/projecteru2/core/strategy"
	"github.com/projecteru2/core/types"

	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/assert"
)

func TestCalculateCapacity(t *testing.T) {
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
		ResourceOpts: map[string]interface{}{
			"cpu": 1.2,
			"mem": "100000PB",
		},
		DeployStrategy: strategy.Auto,
	}

	msg, err := c.CalculateCapacity(ctx, opts)
	assert.Nil(t, err)
	fmt.Println(litter.Sdump(msg))
}
