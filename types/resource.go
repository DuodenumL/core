package types

// ResourceMeta for messages and workload to store
type ResourceMeta map[string]WorkloadResourceArgs

// RawParams .
type RawParams map[string]interface{}

// NodeResourceOpts .
type NodeResourceOpts RawParams

// NodeResourceArgs .
type NodeResourceArgs RawParams

// WorkloadResourceOpts .
type WorkloadResourceOpts RawParams

// WorkloadResourceArgs .
type WorkloadResourceArgs RawParams

// EngineArgs .
type EngineArgs RawParams
