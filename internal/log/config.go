package log

import "github.com/hashicorp/raft"

// Config centralizes the log's configuration, making
// configuration of logs throughout the code easy to do.
type Config struct {
	Raft struct {
		raft.Config
		StreamLayer *StreamLayer
		Bootstrap bool
}

	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
