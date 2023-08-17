// Code generated by mockery v2.20.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// FailureDetector is an autogenerated mock type for the FailureDetector type
type FailureDetector struct {
	mock.Mock
}

// Complain provides a mock function with given fields: viewNum, stopView
func (_m *FailureDetector) Complain(viewNum uint64, stopView bool) {
	_m.Called(viewNum, stopView)
}

type mockConstructorTestingTNewFailureDetector interface {
	mock.TestingT
	Cleanup(func())
}

// NewFailureDetector creates a new instance of FailureDetector. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewFailureDetector(t mockConstructorTestingTNewFailureDetector) *FailureDetector {
	mock := &FailureDetector{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
