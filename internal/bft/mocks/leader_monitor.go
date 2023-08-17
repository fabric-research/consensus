// Code generated by mockery v2.20.0. DO NOT EDIT.

package mocks

import (
	bft "github.com/SmartBFT-Go/consensus/internal/bft"
	mock "github.com/stretchr/testify/mock"

	smartbftprotos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

// LeaderMonitor is an autogenerated mock type for the LeaderMonitor type
type LeaderMonitor struct {
	mock.Mock
}

// ChangeRole provides a mock function with given fields: role, view, leaderID
func (_m *LeaderMonitor) ChangeRole(role bft.Role, view uint64, leaderID uint64) {
	_m.Called(role, view, leaderID)
}

// Close provides a mock function with given fields:
func (_m *LeaderMonitor) Close() {
	_m.Called()
}

// HeartbeatWasSent provides a mock function with given fields:
func (_m *LeaderMonitor) HeartbeatWasSent() {
	_m.Called()
}

// InjectArtificialHeartbeat provides a mock function with given fields: sender, msg
func (_m *LeaderMonitor) InjectArtificialHeartbeat(sender uint64, msg *smartbftprotos.Message) {
	_m.Called(sender, msg)
}

// ProcessMsg provides a mock function with given fields: sender, msg
func (_m *LeaderMonitor) ProcessMsg(sender uint64, msg *smartbftprotos.Message) {
	_m.Called(sender, msg)
}

// StopLeaderSendMsg provides a mock function with given fields:
func (_m *LeaderMonitor) StopLeaderSendMsg() {
	_m.Called()
}

type mockConstructorTestingTNewLeaderMonitor interface {
	mock.TestingT
	Cleanup(func())
}

// NewLeaderMonitor creates a new instance of LeaderMonitor. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewLeaderMonitor(t mockConstructorTestingTNewLeaderMonitor) *LeaderMonitor {
	mock := &LeaderMonitor{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
