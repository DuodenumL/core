package etcdv3

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/projecteru2/core/log"
	"github.com/projecteru2/core/strategy"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// MakeDeployStatus get deploy status from store
func (m *Mercury) MakeDeployStatus(ctx context.Context, appname, entryname string, strategyInfos []strategy.Info) error {
	// 手动加 / 防止不精确
	key := filepath.Join(workloadDeployPrefix, appname, entryname) + "/"
	resp, err := m.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		log.Warnf(ctx, "[MakeDeployStatus] Deploy status not found %s.%s", appname, entryname)
	}
	if err = m.doGetDeployStatus(ctx, resp, strategyInfos); err != nil {
		return err
	}
	return m.doLoadProcessing(ctx, appname, entryname, strategyInfos)
}

// GetDeployStatus get deploy status from store
func (m *Mercury) GetDeployStatus(ctx context.Context, appname, entryname string) (map[string]int, error) {
	// 手动加 / 防止不精确
	key := filepath.Join(workloadDeployPrefix, appname, entryname) + "/"
	resp, err := m.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		log.Warnf(ctx, "[MakeDeployStatus] Deploy status not found %s.%s", appname, entryname)
	}

	deployCount, err := m.doGetDeployStatusCount(ctx, resp)
	if err != nil {
		return nil, err
	}

	processingCount, err := m.doLoadProcessingCount(ctx, appname, entryname)
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
func (m *Mercury) doGetDeployStatusCount(_ context.Context, resp *clientv3.GetResponse) (map[string]int, error) {
	nodesCount := map[string]int{}
	for _, ev := range resp.Kvs {
		key := string(ev.Key)
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

func (m *Mercury) doGetDeployStatus(_ context.Context, resp *clientv3.GetResponse, strategyInfos []strategy.Info) error {
	nodesCount := map[string]int{}
	for _, ev := range resp.Kvs {
		key := string(ev.Key)
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
