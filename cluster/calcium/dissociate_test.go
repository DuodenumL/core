package calcium

import (
	"context"
	"fmt"
	"testing"

	storemocks "github.com/projecteru2/core/store/mocks"

	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/assert"
)

func TestDissociateWorkload(t *testing.T) {
	c := NewTestCluster()
	c.store = storemocks.FromTemplate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ids := createMockWorkloadWithResourcePlugin(t, ctx, c)

	ch, err := c.DissociateWorkload(ctx, ids)
	assert.Nil(t, err)

	for msg := range ch {
		fmt.Println(litter.Sdump(msg))
	}
}
