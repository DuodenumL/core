package calcalcium

import (
	"context"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/types"
	"github.com/projecteru2/core/utils"
)

func (c *Calcium) doRemapResourceAndLog(ctx context.Context, logger log.Fields, node *types.Node) {
	log.Debugf(ctx, "[doRemapResourceAndLog] remap node %s", node.Name)
	ctx, cancel := context.WithTimeout(utils.InheritTracingInfo(ctx, context.TODO()), c.config.GlobalTimeout)
	defer cancel()
	logger = logger.WithField("Calcium", "doRemapResourceAndLog").WithField("nodename", node.Name)
	//if ch, err := c.remapResource(ctx, node); logger.Err(ctx, err) == nil {
	//	for msg := range ch {
	//		logger.WithField("id", msg.ID).Err(ctx, msg.Error) // nolint:errcheck
	//	}
	//}
}
