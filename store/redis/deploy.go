package redis

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/strategy"
)

// MakeDeployStatus get deploy status from store
func (r *Rediaron) MakeDeployStatus(ctx context.Context, appname, entryname string, strategyInfos []strategy.Info) error {
	// 手动加 / 防止不精确
	key := filepath.Join(workloadDeployPrefix, appname, entryname) + "/*"
	data, err := r.getByKeyPattern(ctx, key, 0)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		log.Warnf(ctx, "[MakeDeployStatus] Deploy status not found %s.%s", appname, entryname)
	}
	if err = r.doGetDeployStatus(ctx, data, strategyInfos); err != nil {
		return err
	}
	return r.doLoadProcessing(ctx, appname, entryname, strategyInfos)
}

// GetDeployStatus .
func (r *Rediaron) GetDeployStatus(ctx context.Context, appname, entryname string) (map[string]int, error) {
	// 手动加 / 防止不精确
	key := filepath.Join(workloadDeployPrefix, appname, entryname) + "/*"
	data, err := r.getByKeyPattern(ctx, key, 0)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		log.Warnf(ctx, "[MakeDeployStatus] Deploy status not found %s.%s", appname, entryname)
	}

	deployCount, err := r.doGetDeployStatusCount(ctx, data)
	if err != nil {
		return nil, err
	}
	processingCount, err := r.doLoadProcessingCount(ctx, appname, entryname)
	if err != nil {
		return nil, err
	}

	// node count: deploy count + processing count
	nodeCount := map[string]int{}
	for node, count := range deployCount {
		nodeCount[node] = count
	}
	for node, count := range processingCount {
		nodeCount[node] += count
	}

	return nodeCount, nil
}

// doGetDeployStatusCount returns how many workload have been deployed on each node
func (r *Rediaron) doGetDeployStatusCount(_ context.Context, data map[string]string) (map[string]int, error) {
	nodesCount := map[string]int{}
	for key := range data {
		parts := strings.Split(key, "/")
		nodename := parts[len(parts)-2]
		if _, ok := nodesCount[nodename]; !ok {
			nodesCount[nodename] = 1
			continue
		}
		nodesCount[nodename]++
	}

	return nodesCount, nil
}

func (r *Rediaron) doGetDeployStatus(_ context.Context, data map[string]string, strategyInfos []strategy.Info) error {
	nodesCount := map[string]int{}
	for key := range data {
		parts := strings.Split(key, "/")
		nodename := parts[len(parts)-2]
		if _, ok := nodesCount[nodename]; !ok {
			nodesCount[nodename] = 1
			continue
		}
		nodesCount[nodename]++
	}

	setCount(nodesCount, strategyInfos)
	return nil
}
