package mocks

import (
	"context"
	"github.com/projecteru2/core/lock"
	"github.com/stretchr/testify/mock"
)

func FromTemplate() lock.DistributedLock {
	m := &DistributedLock{}
	m.On("Lock", mock.Anything).Return(context.Background(), nil)
	m.On("TryLock", mock.Anything).Return(context.Background(), nil)
	m.On("Unlock", mock.Anything).Return(nil)
	return m
}
