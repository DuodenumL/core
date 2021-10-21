package calcalcium

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

func TestDissociateWorkload(t *testing.T) {
	c := NewTestCluster()
	c.store = storemocks.FromTemplate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.resource = resources.NewPluginManager(ctx, c.config)
	c.resource.AddPlugins(mocks.NewMockCpuPlugin(), mocks.NewMockMemPlugin())

	ids := createMockWorkloadWithResourcePlugin(t, ctx, c)

	ch, err := c.DissociateWorkload(ctx, ids)
	assert.Nil(t, err)

	for msg := range ch {
		fmt.Println(litter.Sdump(msg))
	}
}
