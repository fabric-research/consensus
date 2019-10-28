// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
)

// A node could either be a leader or a follower
const (
	Leader   Role = false
	Follower Role = true
)

//go:generate mockery -dir . -name HeartbeatEventHandler -case underscore -output ./mocks/

// HeartbeatEventHandler defines who to call when a heartbeat timeout expires or a Sync needs to be triggered.
// This is implemented by the Controller.
type HeartbeatEventHandler interface {
	// OnHeartbeatTimeout is called when a heartbeat timeout expires.
	OnHeartbeatTimeout(view uint64, leaderID uint64)
	// Sync is called when enough heartbeat responses report that the current leader's view is outdated.
	Sync()
}

// Role indicates if this node is a follower or a leader
type Role bool

type roleChange struct {
	view     uint64
	leaderID uint64
	follower Role
}

// heartbeatResponseCollector is a map from node ID to view number, and hold the last response from each node.
type heartbeatResponseCollector map[uint64]uint64

// HeartbeatMonitor implements LeaderMonitor
type HeartbeatMonitor struct {
	scheduler           <-chan time.Time
	inc                 chan incMsg
	stopChan            chan struct{}
	commandChan         chan roleChange
	logger              api.Logger
	hbTimeout           time.Duration
	hbCount             int
	comm                Comm
	numberOfNodes       uint64
	handler             HeartbeatEventHandler
	view                uint64
	leaderID            uint64
	follower            Role
	lastHeartbeat       time.Time
	lastTick            time.Time
	hbRespCollector     heartbeatResponseCollector
	running             sync.WaitGroup
	runOnce             sync.Once
	timedOut            bool
	syncReq             bool
	viewSequences       *atomic.Value
	sentHeartbeat       chan struct{}
	artificialHeartbeat chan incMsg
}

// NewHeartbeatMonitor creates a new HeartbeatMonitor
func NewHeartbeatMonitor(
	scheduler <-chan time.Time,
	logger api.Logger,
	heartbeatTimeout time.Duration,
	heartbeatCount int,
	comm Comm,
	numberOfNodes uint64,
	handler HeartbeatEventHandler,
	viewSequences *atomic.Value,
) *HeartbeatMonitor {
	hm := &HeartbeatMonitor{
		stopChan:            make(chan struct{}),
		inc:                 make(chan incMsg),
		commandChan:         make(chan roleChange),
		scheduler:           scheduler,
		logger:              logger,
		hbTimeout:           heartbeatTimeout,
		hbCount:             heartbeatCount,
		comm:                comm,
		numberOfNodes:       numberOfNodes,
		handler:             handler,
		hbRespCollector:     make(heartbeatResponseCollector),
		viewSequences:       viewSequences,
		sentHeartbeat:       make(chan struct{}, 1),
		artificialHeartbeat: make(chan incMsg, 1),
	}
	return hm
}

func (hm *HeartbeatMonitor) start() {
	hm.running.Add(1)
	go hm.run()
}

// Close stops following or sending heartbeats.
func (hm *HeartbeatMonitor) Close() {
	if hm.closed() {
		return
	}

	defer hm.running.Wait()
	close(hm.stopChan)
}

func (hm *HeartbeatMonitor) run() {
	defer hm.running.Done()
	for {
		select {
		case <-hm.stopChan:
			return
		case now := <-hm.scheduler:
			hm.tick(now)
		case msg := <-hm.inc:
			hm.handleMsg(msg.sender, msg.Message)
		case cmd := <-hm.commandChan:
			hm.handleCommand(cmd)
		case <-hm.sentHeartbeat:
			hm.lastHeartbeat = hm.lastTick
		case msg := <-hm.artificialHeartbeat:
			hm.handleMsg(msg.sender, msg.Message)
		}
	}
}

// ProcessMsg handles an incoming heartbeat or heartbeat-response.
// If the sender and msg.View equal what we expect, and the timeout had not expired yet, the timeout is extended.
func (hm *HeartbeatMonitor) ProcessMsg(sender uint64, msg *smartbftprotos.Message) {
	select {
	case hm.inc <- incMsg{
		sender:  sender,
		Message: msg,
	}:
	case <-hm.stopChan:
	}
}

// InjectArtificialHeartbeat injects an artificial heartbeat to the monitor
func (hm *HeartbeatMonitor) InjectArtificialHeartbeat(sender uint64, msg *smartbftprotos.Message) {
	select {
	case hm.artificialHeartbeat <- incMsg{
		sender:  sender,
		Message: msg,
	}:
	default:
	}
}

// ChangeRole will change the role of this HeartbeatMonitor
func (hm *HeartbeatMonitor) ChangeRole(follower Role, view uint64, leaderID uint64) {
	hm.runOnce.Do(func() {
		hm.follower = follower
		hm.start()
	})

	role := "leader"
	if follower {
		role = "follower"
	}

	hm.logger.Infof("Changing to %s role, current view: %d, current leader: %d", role, view, leaderID)
	select {
	case hm.commandChan <- roleChange{
		leaderID: leaderID,
		view:     view,
		follower: follower,
	}:
	case <-hm.stopChan:
		return
	}

}

func (hm *HeartbeatMonitor) handleMsg(sender uint64, msg *smartbftprotos.Message) {
	switch msg.GetContent().(type) {
	case *smartbftprotos.Message_HeartBeat:
		hm.handleHeartBeat(sender, msg.GetHeartBeat())
	case *smartbftprotos.Message_HeartBeatResponse:
		hm.handleHeartBeatResponse(sender, msg.GetHeartBeatResponse())
	default:
		hm.logger.Warnf("Unexpected message type, ignoring")
	}
}

func (hm *HeartbeatMonitor) handleHeartBeat(sender uint64, hb *smartbftprotos.HeartBeat) {
	if hb.View < hm.view {
		hm.logger.Debugf("Heartbeat view is lower than expected, sending response; expected-view=%d, received-view: %d", hm.view, hb.View)
		hm.sendHeartBeatResponse(sender)
		return
	}

	if sender != hm.leaderID {
		hm.logger.Debugf("Heartbeat sender is not leader, ignoring; leader: %d, sender: %d", hm.leaderID, sender)
		return
	}

	if hb.View > hm.view {
		hm.logger.Debugf("Heartbeat view is bigger than expected, syncing and ignoring; expected-view=%d, received-view: %d", hm.view, hb.View)
		hm.handler.Sync()
		return
	}

	if hm.viewActiveButBehindLeader(hb) {
		hm.logger.Debugf("Heartbeat sequence is bigger than expected, syncing and ignoring")
		hm.handler.Sync()
		return
	}

	hm.logger.Debugf("Received heartbeat from %d, last heartbeat was %v ago", sender, hm.lastTick.Sub(hm.lastHeartbeat))
	hm.lastHeartbeat = hm.lastTick
}

// handleHeartBeatResponse keeps track of responses, and if we get f+1 identical, force a sync
func (hm *HeartbeatMonitor) handleHeartBeatResponse(sender uint64, hbr *smartbftprotos.HeartBeatResponse) {
	if hm.follower {
		hm.logger.Debugf("Monitor is not a leader, ignoring HeartBeatResponse; sender: %d, msg: %v", sender, hbr)
		return
	}

	if hm.syncReq {
		hm.logger.Debugf("Monitor already called Sync, ignoring HeartBeatResponse; sender: %d, msg: %v", sender, hbr)
		return
	}

	if hm.view >= hbr.View {
		hm.logger.Debugf("Monitor view: %d >= HeartBeatResponse, ignoring; sender: %d, msg: %v", hm.view, sender, hbr)
		return
	}

	hm.logger.Debugf("Received HeartBeatResponse, msg: %v; from %d", hbr, sender)
	hm.hbRespCollector[sender] = hbr.View

	// check if we have f+1 votes
	_, f := computeQuorum(hm.numberOfNodes)
	if len(hm.hbRespCollector) >= f+1 {
		hm.logger.Infof("Received HeartBeatResponse triggered a call to HeartBeatEventHandler Sync, view: %d", hbr.View)
		hm.handler.Sync()
		hm.syncReq = true
	}
}

func (hm *HeartbeatMonitor) sendHeartBeatResponse(target uint64) {
	heartbeatResponse := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeatResponse{
			HeartBeatResponse: &smartbftprotos.HeartBeatResponse{
				View: hm.view,
			},
		},
	}
	hm.comm.SendConsensus(target, heartbeatResponse)
	hm.logger.Debugf("Sent HeartBeatResponse view: %d; to %d", hm.view, target)
}

func (hm *HeartbeatMonitor) viewActiveButBehindLeader(hbMsg *smartbftprotos.HeartBeat) bool {
	vs := hm.viewSequences.Load()
	// View isn't initialized
	if vs == nil {
		return false
	}

	viewSeq := vs.(ViewSequence)
	if !viewSeq.ViewActive {
		return false
	}

	ourSeq := viewSeq.ProposalSeq

	areWeBehind := ourSeq+1 < hbMsg.Seq

	if areWeBehind {
		hm.logger.Debugf("Leader's sequence is %d and ours is %d", hbMsg.Seq, ourSeq)
	}

	return areWeBehind
}

func (hm *HeartbeatMonitor) tick(now time.Time) {
	hm.lastTick = now
	if hm.lastHeartbeat.IsZero() {
		hm.lastHeartbeat = now
	}
	if hm.follower {
		hm.followerTick(now)
	} else {
		hm.leaderTick(now)
	}
}

func (hm *HeartbeatMonitor) closed() bool {
	select {
	case <-hm.stopChan:
		return true
	default:
		return false
	}
}

func (hm *HeartbeatMonitor) handleCommand(cmd roleChange) {
	hm.timedOut = false
	hm.view = cmd.view
	hm.leaderID = cmd.leaderID
	hm.follower = cmd.follower
	hm.lastHeartbeat = hm.lastTick
	hm.hbRespCollector = make(heartbeatResponseCollector)
	hm.syncReq = false
}

func (hm *HeartbeatMonitor) leaderTick(now time.Time) {
	if now.Sub(hm.lastHeartbeat)*time.Duration(hm.hbCount) < hm.hbTimeout {
		return
	}

	var sequence uint64
	vs := hm.viewSequences.Load()
	if vs != nil && vs.(ViewSequence).ViewActive {
		sequence = vs.(ViewSequence).ProposalSeq
	} else {
		hm.logger.Infof("ViewSequence uninitialized or view inactive")
		return
	}
	hm.logger.Debugf("Sending heartbeat with view %d, sequence %d", hm.view, sequence)
	heartbeat := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeat{
			HeartBeat: &smartbftprotos.HeartBeat{
				View: hm.view,
				Seq:  sequence,
			},
		},
	}
	hm.comm.BroadcastConsensus(heartbeat)
	hm.lastHeartbeat = now
}

func (hm *HeartbeatMonitor) followerTick(now time.Time) {
	if hm.timedOut || hm.lastHeartbeat.IsZero() {
		hm.lastHeartbeat = now
		return
	}

	delta := now.Sub(hm.lastHeartbeat)
	if delta >= hm.hbTimeout {
		hm.logger.Warnf("Heartbeat timeout (%v) from %d expired; last heartbeat was observed %s ago",
			hm.hbTimeout, hm.leaderID, delta)
		hm.handler.OnHeartbeatTimeout(hm.view, hm.leaderID)
		hm.timedOut = true
		return
	}

	hm.logger.Debugf("Last heartbeat from %d was %v ago", hm.leaderID, delta)
}

func (hm *HeartbeatMonitor) HeartbeatWasSent() {
	select {
	case hm.sentHeartbeat <- struct{}{}:
	default:
	}
}
