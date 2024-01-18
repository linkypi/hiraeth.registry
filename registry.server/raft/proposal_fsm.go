package raft

import (
	"github.com/hashicorp/raft"
	"io"
)

type PropFsm struct {
}

// Asserting PropFsm implements raft. FSM interface
var _ raft.FSM = &PropFsm{}

func (f *PropFsm) Apply(l *raft.Log) interface{} {

	return nil
}

func (f *PropFsm) Snapshot() (raft.FSMSnapshot, error) {
	// Make sure that any future calls to f.Apply() don't change the snapshot.
	return &snapshot{}, nil
}

func (f *PropFsm) Restore(r io.ReadCloser) error {

	return nil
}

type snapshot struct {
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {

	return sink.Close()
}

func (s *snapshot) Release() {
}
