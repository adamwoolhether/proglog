package log

// Config centralizes the log's configuration, making
// configuration of logs throughout the code easy to do.
type Config struct {
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
