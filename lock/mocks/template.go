package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/projecteru2/core/lock"
)

func FromTemplate() lock.DistributedLock {
	m := &DistributedLock{}
	m.On("Lock", mock.Anything).Return(context.Background(), nil)
	m.On("TryLock", mock.Anything).Return(context.Background(), nil)
	m.On("Unlock", mock.Anything).Return(nil)
	return m
}
