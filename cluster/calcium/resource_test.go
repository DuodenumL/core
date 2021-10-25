package calcium

import (
	"context"
	"fmt"
	"testing"

	storemocks "github.com/projecteru2/core/store/mocks"

	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/assert"
)

func TestPodResource(t *testing.T) {
	c := NewTestCluster()
	c.store = storemocks.FromTemplate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createMockWorkloadWithResourcePlugin(t, ctx, c)

	res, err := c.PodResource(ctx, "somepod")
	assert.Nil(t, err)
	fmt.Println(litter.Sdump(res))
}
