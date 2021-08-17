package log

import (
	"bytes"
	"crypto/tls"
	"os"
	"path/filepath"
	"time"

	raftboltdb "github.com/hashicorp/raft-boltdb"

	"github.com/hashicorp/raft"
)

type DistributedLog struct {
	config Config
	log *Log
	raft *raft.Raft
}

// NewDistributedLog defines and creates the log,
// deferring logic to the setup methods. The distributed,
// replicated Raft log will exist alongside our
// single-server, non-replicated log also build in
// package log.
func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error){
	l := &DistributedLog{
		config: config,
	}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return nil, nil
}

// setupLog creates the log where the server will store the
// user's records.
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

// setupRaft configures and creates the server's Raft instance.
// It creates a finite-state machine (FSM), and the log store.
// Initial offset of 1 is required by Raft.
func(l *DistributedLog) setupRaft(dataDir string) error {
	fsm := &fsm{log: l.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// stableStore is a key-value store for important metadata, using Bolt
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	// snapshotStore is for recovery and restoration, lessning load from the leader, we retain 1
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"), retain, os.Stderr)
	if err != nil {
		return err
	}

	// our transport wraps a low-level stream layer abstraction
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(l.config.Raft.StreamLayer, maxPool, timeout, os.Stderr)

	// config's LocalID is the unique ID for this server, all other config is optional.
	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID: config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err


}

// type fsm struct {
// 	log *Log
// }
//
// func newLogStore(dir string, c Config) (*logStore, error) {
//
// }
//
// type logStore struct {
// 	*Log
// }