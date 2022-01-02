// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import (
	context "context"

	io "io"

	mock "github.com/stretchr/testify/mock"

	source "github.com/projecteru2/core/source"

	time "time"

	types "github.com/projecteru2/core/engine/types"
)

// API is an autogenerated mock type for the API type
type API struct {
	mock.Mock
}

// BuildContent provides a mock function with given fields: ctx, scm, opts
func (_m *API) BuildContent(ctx context.Context, scm source.Source, opts *types.BuildContentOptions) (string, io.Reader, error) {
	ret := _m.Called(ctx, scm, opts)

	var r0 string
	if rf, ok := ret.Get(0).(func(context.Context, source.Source, *types.BuildContentOptions) string); ok {
		r0 = rf(ctx, scm, opts)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 io.Reader
	if rf, ok := ret.Get(1).(func(context.Context, source.Source, *types.BuildContentOptions) io.Reader); ok {
		r1 = rf(ctx, scm, opts)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(io.Reader)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, source.Source, *types.BuildContentOptions) error); ok {
		r2 = rf(ctx, scm, opts)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// BuildRefs provides a mock function with given fields: ctx, opts
func (_m *API) BuildRefs(ctx context.Context, opts *types.BuildRefOptions) []string {
	ret := _m.Called(ctx, opts)

	var r0 []string
	if rf, ok := ret.Get(0).(func(context.Context, *types.BuildRefOptions) []string); ok {
		r0 = rf(ctx, opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	return r0
}

// ExecExitCode provides a mock function with given fields: ctx, ID, result
func (_m *API) ExecExitCode(ctx context.Context, ID string, result string) (int, error) {
	ret := _m.Called(ctx, ID, result)

	var r0 int
	if rf, ok := ret.Get(0).(func(context.Context, string, string) int); ok {
		r0 = rf(ctx, ID, result)
	} else {
		r0 = ret.Get(0).(int)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, ID, result)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ExecResize provides a mock function with given fields: ctx, ID, result, height, width
func (_m *API) ExecResize(ctx context.Context, ID string, result string, height uint, width uint) error {
	ret := _m.Called(ctx, ID, result, height, width)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, uint, uint) error); ok {
		r0 = rf(ctx, ID, result, height, width)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Execute provides a mock function with given fields: ctx, ID, config
func (_m *API) Execute(ctx context.Context, ID string, config *types.ExecConfig) (string, io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
	ret := _m.Called(ctx, ID, config)

	var r0 string
	if rf, ok := ret.Get(0).(func(context.Context, string, *types.ExecConfig) string); ok {
		r0 = rf(ctx, ID, config)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 io.ReadCloser
	if rf, ok := ret.Get(1).(func(context.Context, string, *types.ExecConfig) io.ReadCloser); ok {
		r1 = rf(ctx, ID, config)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(io.ReadCloser)
		}
	}

	var r2 io.ReadCloser
	if rf, ok := ret.Get(2).(func(context.Context, string, *types.ExecConfig) io.ReadCloser); ok {
		r2 = rf(ctx, ID, config)
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).(io.ReadCloser)
		}
	}

	var r3 io.WriteCloser
	if rf, ok := ret.Get(3).(func(context.Context, string, *types.ExecConfig) io.WriteCloser); ok {
		r3 = rf(ctx, ID, config)
	} else {
		if ret.Get(3) != nil {
			r3 = ret.Get(3).(io.WriteCloser)
		}
	}

	var r4 error
	if rf, ok := ret.Get(4).(func(context.Context, string, *types.ExecConfig) error); ok {
		r4 = rf(ctx, ID, config)
	} else {
		r4 = ret.Error(4)
	}

	return r0, r1, r2, r3, r4
}

// ImageBuild provides a mock function with given fields: ctx, input, refs
func (_m *API) ImageBuild(ctx context.Context, input io.Reader, refs []string) (io.ReadCloser, error) {
	ret := _m.Called(ctx, input, refs)

	var r0 io.ReadCloser
	if rf, ok := ret.Get(0).(func(context.Context, io.Reader, []string) io.ReadCloser); ok {
		r0 = rf(ctx, input, refs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(io.ReadCloser)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, io.Reader, []string) error); ok {
		r1 = rf(ctx, input, refs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ImageBuildCachePrune provides a mock function with given fields: ctx, all
func (_m *API) ImageBuildCachePrune(ctx context.Context, all bool) (uint64, error) {
	ret := _m.Called(ctx, all)

	var r0 uint64
	if rf, ok := ret.Get(0).(func(context.Context, bool) uint64); ok {
		r0 = rf(ctx, all)
	} else {
		r0 = ret.Get(0).(uint64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, bool) error); ok {
		r1 = rf(ctx, all)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ImageBuildFromExist provides a mock function with given fields: ctx, ID, refs, user
func (_m *API) ImageBuildFromExist(ctx context.Context, ID string, refs []string, user string) (string, error) {
	ret := _m.Called(ctx, ID, refs, user)

	var r0 string
	if rf, ok := ret.Get(0).(func(context.Context, string, []string, string) string); ok {
		r0 = rf(ctx, ID, refs, user)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, []string, string) error); ok {
		r1 = rf(ctx, ID, refs, user)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ImageList provides a mock function with given fields: ctx, image
func (_m *API) ImageList(ctx context.Context, image string) ([]*types.Image, error) {
	ret := _m.Called(ctx, image)

	var r0 []*types.Image
	if rf, ok := ret.Get(0).(func(context.Context, string) []*types.Image); ok {
		r0 = rf(ctx, image)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*types.Image)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, image)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ImageLocalDigests provides a mock function with given fields: ctx, image
func (_m *API) ImageLocalDigests(ctx context.Context, image string) ([]string, error) {
	ret := _m.Called(ctx, image)

	var r0 []string
	if rf, ok := ret.Get(0).(func(context.Context, string) []string); ok {
		r0 = rf(ctx, image)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, image)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ImagePull provides a mock function with given fields: ctx, ref, all
func (_m *API) ImagePull(ctx context.Context, ref string, all bool) (io.ReadCloser, error) {
	ret := _m.Called(ctx, ref, all)

	var r0 io.ReadCloser
	if rf, ok := ret.Get(0).(func(context.Context, string, bool) io.ReadCloser); ok {
		r0 = rf(ctx, ref, all)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(io.ReadCloser)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, bool) error); ok {
		r1 = rf(ctx, ref, all)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ImagePush provides a mock function with given fields: ctx, ref
func (_m *API) ImagePush(ctx context.Context, ref string) (io.ReadCloser, error) {
	ret := _m.Called(ctx, ref)

	var r0 io.ReadCloser
	if rf, ok := ret.Get(0).(func(context.Context, string) io.ReadCloser); ok {
		r0 = rf(ctx, ref)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(io.ReadCloser)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, ref)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ImageRemoteDigest provides a mock function with given fields: ctx, image
func (_m *API) ImageRemoteDigest(ctx context.Context, image string) (string, error) {
	ret := _m.Called(ctx, image)

	var r0 string
	if rf, ok := ret.Get(0).(func(context.Context, string) string); ok {
		r0 = rf(ctx, image)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, image)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ImageRemove provides a mock function with given fields: ctx, image, force, prune
func (_m *API) ImageRemove(ctx context.Context, image string, force bool, prune bool) ([]string, error) {
	ret := _m.Called(ctx, image, force, prune)

	var r0 []string
	if rf, ok := ret.Get(0).(func(context.Context, string, bool, bool) []string); ok {
		r0 = rf(ctx, image, force, prune)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, bool, bool) error); ok {
		r1 = rf(ctx, image, force, prune)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ImagesPrune provides a mock function with given fields: ctx
func (_m *API) ImagesPrune(ctx context.Context) error {
	ret := _m.Called(ctx)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Info provides a mock function with given fields: ctx
func (_m *API) Info(ctx context.Context) (*types.Info, error) {
	ret := _m.Called(ctx)

	var r0 *types.Info
	if rf, ok := ret.Get(0).(func(context.Context) *types.Info); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*types.Info)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NetworkConnect provides a mock function with given fields: ctx, network, target, ipv4, ipv6
func (_m *API) NetworkConnect(ctx context.Context, network string, target string, ipv4 string, ipv6 string) ([]string, error) {
	ret := _m.Called(ctx, network, target, ipv4, ipv6)

	var r0 []string
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string) []string); ok {
		r0 = rf(ctx, network, target, ipv4, ipv6)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string, string, string) error); ok {
		r1 = rf(ctx, network, target, ipv4, ipv6)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NetworkDisconnect provides a mock function with given fields: ctx, network, target, force
func (_m *API) NetworkDisconnect(ctx context.Context, network string, target string, force bool) error {
	ret := _m.Called(ctx, network, target, force)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, bool) error); ok {
		r0 = rf(ctx, network, target, force)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NetworkList provides a mock function with given fields: ctx, drivers
func (_m *API) NetworkList(ctx context.Context, drivers []string) ([]*types.Network, error) {
	ret := _m.Called(ctx, drivers)

	var r0 []*types.Network
	if rf, ok := ret.Get(0).(func(context.Context, []string) []*types.Network); ok {
		r0 = rf(ctx, drivers)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*types.Network)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []string) error); ok {
		r1 = rf(ctx, drivers)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ResourceValidate provides a mock function with given fields: ctx, cpu, cpumap, memory, storage
func (_m *API) ResourceValidate(ctx context.Context, cpu float64, cpumap map[string]int64, memory int64, storage int64) error {
	ret := _m.Called(ctx, cpu, cpumap, memory, storage)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, float64, map[string]int64, int64, int64) error); ok {
		r0 = rf(ctx, cpu, cpumap, memory, storage)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// VirtualizationAttach provides a mock function with given fields: ctx, ID, stream, openStdin
func (_m *API) VirtualizationAttach(ctx context.Context, ID string, stream bool, openStdin bool) (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
	ret := _m.Called(ctx, ID, stream, openStdin)

	var r0 io.ReadCloser
	if rf, ok := ret.Get(0).(func(context.Context, string, bool, bool) io.ReadCloser); ok {
		r0 = rf(ctx, ID, stream, openStdin)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(io.ReadCloser)
		}
	}

	var r1 io.ReadCloser
	if rf, ok := ret.Get(1).(func(context.Context, string, bool, bool) io.ReadCloser); ok {
		r1 = rf(ctx, ID, stream, openStdin)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(io.ReadCloser)
		}
	}

	var r2 io.WriteCloser
	if rf, ok := ret.Get(2).(func(context.Context, string, bool, bool) io.WriteCloser); ok {
		r2 = rf(ctx, ID, stream, openStdin)
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).(io.WriteCloser)
		}
	}

	var r3 error
	if rf, ok := ret.Get(3).(func(context.Context, string, bool, bool) error); ok {
		r3 = rf(ctx, ID, stream, openStdin)
	} else {
		r3 = ret.Error(3)
	}

	return r0, r1, r2, r3
}

// VirtualizationCopyFrom provides a mock function with given fields: ctx, ID, path
func (_m *API) VirtualizationCopyFrom(ctx context.Context, ID string, path string) ([]byte, int, int, int64, error) {
	ret := _m.Called(ctx, ID, path)

	var r0 []byte
	if rf, ok := ret.Get(0).(func(context.Context, string, string) []byte); ok {
		r0 = rf(ctx, ID, path)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	var r1 int
	if rf, ok := ret.Get(1).(func(context.Context, string, string) int); ok {
		r1 = rf(ctx, ID, path)
	} else {
		r1 = ret.Get(1).(int)
	}

	var r2 int
	if rf, ok := ret.Get(2).(func(context.Context, string, string) int); ok {
		r2 = rf(ctx, ID, path)
	} else {
		r2 = ret.Get(2).(int)
	}

	var r3 int64
	if rf, ok := ret.Get(3).(func(context.Context, string, string) int64); ok {
		r3 = rf(ctx, ID, path)
	} else {
		r3 = ret.Get(3).(int64)
	}

	var r4 error
	if rf, ok := ret.Get(4).(func(context.Context, string, string) error); ok {
		r4 = rf(ctx, ID, path)
	} else {
		r4 = ret.Error(4)
	}

	return r0, r1, r2, r3, r4
}

// VirtualizationCopyTo provides a mock function with given fields: ctx, ID, target, content, uid, gid, mode
func (_m *API) VirtualizationCopyTo(ctx context.Context, ID string, target string, content []byte, uid int, gid int, mode int64) error {
	ret := _m.Called(ctx, ID, target, content, uid, gid, mode)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, []byte, int, int, int64) error); ok {
		r0 = rf(ctx, ID, target, content, uid, gid, mode)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// VirtualizationCreate provides a mock function with given fields: ctx, opts
func (_m *API) VirtualizationCreate(ctx context.Context, opts *types.VirtualizationCreateOptions) (*types.VirtualizationCreated, error) {
	ret := _m.Called(ctx, opts)

	var r0 *types.VirtualizationCreated
	if rf, ok := ret.Get(0).(func(context.Context, *types.VirtualizationCreateOptions) *types.VirtualizationCreated); ok {
		r0 = rf(ctx, opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*types.VirtualizationCreated)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *types.VirtualizationCreateOptions) error); ok {
		r1 = rf(ctx, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// VirtualizationInspect provides a mock function with given fields: ctx, ID
func (_m *API) VirtualizationInspect(ctx context.Context, ID string) (*types.VirtualizationInfo, error) {
	ret := _m.Called(ctx, ID)

	var r0 *types.VirtualizationInfo
	if rf, ok := ret.Get(0).(func(context.Context, string) *types.VirtualizationInfo); ok {
		r0 = rf(ctx, ID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*types.VirtualizationInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, ID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// VirtualizationLogs provides a mock function with given fields: ctx, opts
func (_m *API) VirtualizationLogs(ctx context.Context, opts *types.VirtualizationLogStreamOptions) (io.ReadCloser, io.ReadCloser, error) {
	ret := _m.Called(ctx, opts)

	var r0 io.ReadCloser
	if rf, ok := ret.Get(0).(func(context.Context, *types.VirtualizationLogStreamOptions) io.ReadCloser); ok {
		r0 = rf(ctx, opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(io.ReadCloser)
		}
	}

	var r1 io.ReadCloser
	if rf, ok := ret.Get(1).(func(context.Context, *types.VirtualizationLogStreamOptions) io.ReadCloser); ok {
		r1 = rf(ctx, opts)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(io.ReadCloser)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, *types.VirtualizationLogStreamOptions) error); ok {
		r2 = rf(ctx, opts)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// VirtualizationRemove provides a mock function with given fields: ctx, ID, volumes, force
func (_m *API) VirtualizationRemove(ctx context.Context, ID string, volumes bool, force bool) error {
	ret := _m.Called(ctx, ID, volumes, force)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, bool, bool) error); ok {
		r0 = rf(ctx, ID, volumes, force)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// VirtualizationResize provides a mock function with given fields: ctx, ID, height, width
func (_m *API) VirtualizationResize(ctx context.Context, ID string, height uint, width uint) error {
	ret := _m.Called(ctx, ID, height, width)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, uint, uint) error); ok {
		r0 = rf(ctx, ID, height, width)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// VirtualizationResourceRemap provides a mock function with given fields: _a0, _a1
func (_m *API) VirtualizationResourceRemap(_a0 context.Context, _a1 *types.VirtualizationRemapOptions) (<-chan types.VirtualizationRemapMessage, error) {
	ret := _m.Called(_a0, _a1)

	var r0 <-chan types.VirtualizationRemapMessage
	if rf, ok := ret.Get(0).(func(context.Context, *types.VirtualizationRemapOptions) <-chan types.VirtualizationRemapMessage); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan types.VirtualizationRemapMessage)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *types.VirtualizationRemapOptions) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// VirtualizationStart provides a mock function with given fields: ctx, ID
func (_m *API) VirtualizationStart(ctx context.Context, ID string) error {
	ret := _m.Called(ctx, ID)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, ID)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// VirtualizationStop provides a mock function with given fields: ctx, ID, gracefulTimeout
func (_m *API) VirtualizationStop(ctx context.Context, ID string, gracefulTimeout time.Duration) error {
	ret := _m.Called(ctx, ID, gracefulTimeout)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, time.Duration) error); ok {
		r0 = rf(ctx, ID, gracefulTimeout)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// VirtualizationUpdateResource provides a mock function with given fields: ctx, ID, opts
func (_m *API) VirtualizationUpdateResource(ctx context.Context, ID string, opts *types.VirtualizationResource) error {
	ret := _m.Called(ctx, ID, opts)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, *types.VirtualizationResource) error); ok {
		r0 = rf(ctx, ID, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// VirtualizationWait provides a mock function with given fields: ctx, ID, state
func (_m *API) VirtualizationWait(ctx context.Context, ID string, state string) (*types.VirtualizationWaitResult, error) {
	ret := _m.Called(ctx, ID, state)

	var r0 *types.VirtualizationWaitResult
	if rf, ok := ret.Get(0).(func(context.Context, string, string) *types.VirtualizationWaitResult); ok {
		r0 = rf(ctx, ID, state)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*types.VirtualizationWaitResult)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, ID, state)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
