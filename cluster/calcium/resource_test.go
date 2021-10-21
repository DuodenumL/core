package calcium

import (
	"context"
	"fmt"
	"testing"

	"github.com/projecteru2/core/resources"
	"github.com/projecteru2/core/resources/mocks"
	storemocks "github.com/projecteru2/core/store/mocks"

	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/assert"
)

func TestPodResource(t *testing.T) {
	c := NewTestCluster()
	c.store = storemocks.FromTemplate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.resource = resources.NewPluginManager(ctx, c.config)
	c.resource.AddPlugins(mocks.NewMockCpuPlugin(), mocks.NewMockMemPlugin())

	createMockWorkloadWithResourcePlugin(t, ctx, c)

	res, err := c.PodResource(ctx, "somepod")
	assert.Nil(t, err)
	fmt.Println(litter.Sdump(res))
}
