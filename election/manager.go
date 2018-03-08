package election

import (
	"time"
)

const (
	Leader = iota
	Candidate
)

type Manager struct {
	e *Elector
}

type ChangeRoleCallBackOpt func(m *Manager)

func WithChangeToLeaderCallback(cb ChangeRoleCallBack) ChangeRoleCallBackOpt {
	return func(m *Manager) {
		m.e.onChangeToLeader = cb
	}
}

func WithChangeToCandidateCallback(cb ChangeRoleCallBack) ChangeRoleCallBackOpt {
	return func(m *Manager) {
		m.e.onChangeToCandidate = cb
	}
}

func NewElectionManager(servers []string, sessionTimeout time.Duration, path, node, value string) (*Manager, error) {
	e, err := NewElector(servers, sessionTimeout, path, node, value)
	if err != nil {
		return nil, err
	}
	return &Manager{e}, nil
}

func (m *Manager) Join(cbs ...ChangeRoleCallBackOpt) (<-chan LeaderChangeEv, error) {
	for _, cb := range cbs {
		cb(m)
	}
	ev, err := m.e.Start()
	if err != nil {
		return nil, err
	}
	return ev, err
}

func (m *Manager) IsLeader() bool {
	return m.e.IsLeader()
}
