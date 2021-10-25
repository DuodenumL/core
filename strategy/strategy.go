package strategy

import (
	"context"
	"github.com/pkg/errors"
	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/types"
)

const (
	// Auto .
	Auto = "AUTO"
	// Fill .
	Fill = "FILL"
	// Each .
	Each = "EACH"
	// Global .
	Global = "GLOBAL"
	// Dummy for calculate capacity
	Dummy = "DUMMY"
)

// Plans .
var Plans = map[string]strategyFunc{
	Auto:   CommunismPlan,
	Fill:   FillPlan,
	Each:   AveragePlan,
	Global: GlobalPlan,
}

type strategyFunc = func(_ context.Context, _ []Info, need, total, limit int) (map[string]int, error)

// Deploy .
func Deploy(ctx context.Context, opts *types.DeployOptions, strategyInfos []Info, total int) (map[string]int, error) {
	deployMethod, ok := Plans[opts.DeployStrategy]
	if !ok {
		return nil, errors.WithStack(types.ErrBadDeployStrategy)
	}
	if opts.Count <= 0 {
		return nil, errors.WithStack(types.ErrBadCount)
	}

	log.Debugf(ctx, "[strategy.Deploy] infos %+v, need %d, total %d, limit %d", strategyInfos, opts.Count, total, opts.NodesLimit)
	return deployMethod(ctx, strategyInfos, opts.Count, total, opts.NodesLimit)
}

// Info .
type Info struct {
	Nodename string

	Usage float64
	Rate  float64

	Capacity int
	Count    int
}
