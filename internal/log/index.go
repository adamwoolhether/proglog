package log

import (
	"github.com/tysontate/gommap"
	"io"
	"os"
)

// Index entries contain two fields: the record's offset
// and its position in the store file.
var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

// index file consists of a persisted file and a memory mapped file.
// size tells us the seize of the index and where to write
// the next appended entry to the index.
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex creates an index for the given file. It immediately
// saves the file's current size to track the amount of data
// in the file as more entries are added. The file grows to the max
// index size before memory-mapping the file and returning
// the created index to the caller.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read takes an offset and returns the associated record's
// position in the store. Given offset is relative to
// the segment's abse offset. 0 is always the offset of the
// index's first entry, 1 is second entry, etc.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends the given offset and position to the index.
// It first validats that there is enough space to the write
// the entry. If so, the offset and position are encoded and
// written to the memory-mapped file. Lastly, it increments
// the position where the next write will go.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

// Close ensures that memory-mapped file has synced its data
// to persisted file, and the persisted file has flushed its
// contents to the stable storage. It then truncates the
// persisted file to the amount of data that inside it
// before closing the file.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Name will return the index's file path.
func (i *index) Name() string {
	return i.file.Name()
}
