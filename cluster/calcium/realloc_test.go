package calcium

import (
	"context"
	storemocks "github.com/projecteru2/core/store/mocks"
	"github.com/projecteru2/core/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRealloc(t *testing.T) {
	c := NewTestCluster()
	c.store = storemocks.FromTemplate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ids := createMockWorkloadWithResourcePlugin(t, ctx, c)

	opts := &types.ReallocOptions{
		ID:          ids[0],
		ResourceOpts: map[string]interface{}{
			"cpu": 10086,
			"mem": "10086PB",
			"file": []string{"super-cpu", "super-mem"},
		},
	}

	assert.Nil(t, c.ReallocResource(ctx, opts))
}
