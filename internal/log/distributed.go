package log

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/protobuf/proto"
	raftboltdb "github.com/hashicorp/raft-boltdb"

	"github.com/hashicorp/raft"

	api "github.com/adamwoolhether/proglog/api/v1"
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

	l.raft, err = raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
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

// Append appends the record to the log, it tells Raft to apply a command
// using ProduceRequest, which tells FSM to append the reocrd to the log,
// running log replication to replicate the command to the Raft servers.
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(AppendRequestType, &api.ProduceRequest{Record: record})
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

// apply wraps Raft's API to apply requests and return their response.
// It marshals the request type and request into bytes used by Raft
// to record data is replicates.
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	// Apply the command to FSM, replicating the record and appending to the leader's log
	timeout :=10 *time.Second
	future := l.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

// Read reads the recor's offset from the server's log.
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// Define Finite-State Machine, which needs to access the data it manages.
// It must implement three methods: Apply, Snapshot, and Restore.
var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Apply reqds requests, identifies their type, and applies them.
// Currently, there is only one, but other methods can me implemented
// and called upon.
func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}
	return nil
}

// applyAppend unmarshals the request and appeds the record to the
// local log, returning the Raft respones, back to where raft.Apply
// was called in DistributedLog.Append
func (l *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return nil
	}

	offset, err := l.log.Append(req.Record)
	if err != nil {
		return nil
	}

	return &api.ProduceResponse{Offset: offset}
}

// Snapshot returns and FSM snapshot representing a point-in-time
// snapshot of the FSM's state(in this case, the FSM log).
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Persist writes the snapshot's state to a sink, which could be
// in-memory, a file, etc. to store the bytes.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

// Restore is called by Raft to restore an FSM from snapshot.
// It resets the log, configures its initial offset to the
// first record's offset read from the snapshot, and then
// appends the snapshot's record to the new log.
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer

	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(enc.Uint16(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err := f.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

// The following APIs are used by Raft to get records and
// information about the log. To us, our offset is what
// Raft calls 'indexes'.

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.HighestOffset()
	return off, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term: record.Term,
			Type: uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}