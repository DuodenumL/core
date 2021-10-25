package calcium

import (
	"context"
	"testing"

	"github.com/projecteru2/core/log"
	storemocks "github.com/projecteru2/core/store/mocks"
	"github.com/projecteru2/core/strategy"
	"github.com/projecteru2/core/types"

	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/assert"
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
