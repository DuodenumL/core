package types

// ResourceOptions for create/realloc/replace
type ResourceOptions RawParams

// ResourceMeta for messages and workload to store
type ResourceMeta map[string]RawParams

// RawParams .
type RawParams map[string]interface{}
