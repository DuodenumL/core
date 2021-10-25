package calcium

import (
	"context"
	"testing"

	"github.com/projecteru2/core/log"
	storemocks "github.com/projecteru2/core/store/mocks"
	"github.com/stretchr/testify/assert"
)

func TestRemoveWorkloadWithResourcePlugin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewTestCluster()
	c.store = storemocks.FromTemplate()

	ids := createMockWorkloadWithResourcePlugin(t, ctx, c)

	ch, err := c.RemoveWorkload(ctx, ids, true, 1)
	assert.Nil(t, err)
	for msg := range ch {
		log.Infof(ctx, "remove workload msg: %+v", msg)
		assert.True(t, msg.Success)
	}
}
