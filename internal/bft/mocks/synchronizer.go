// Code generated by mockery v2.20.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// Synchronizer is an autogenerated mock type for the Synchronizer type
type Synchronizer struct {
	mock.Mock
}

// Sync provides a mock function with given fields:
func (_m *Synchronizer) Sync() {
	_m.Called()
}

type mockConstructorTestingTNewSynchronizer interface {
	mock.TestingT
	Cleanup(func())
}

// NewSynchronizer creates a new instance of Synchronizer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewSynchronizer(t mockConstructorTestingTNewSynchronizer) *Synchronizer {
	mock := &Synchronizer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
