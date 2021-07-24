package fbptree

import (
	"fmt"
	"io"
	"os"
)

// the size of the first metadata block in the file,
// reserved for different needs
const metadataSize = 1000

// the id of the first free page
const firstFreePageId = uint32(1)
const pageIdSize = 4 // uint32

// pager is an abstaction over the file that represents the file
// as a set of pages. The file is splitten into
// the pages with the fixed size, usually 4096 bytes.
type pager struct {
	file       *os.File
	pageSize   uint16
	freePages  map[uint32]struct{}
	lastPageId uint32
}

type metadata struct {
	pageSize uint16
}

type freePage struct {
	freePages map[uint32]struct{}
	// 0 if does not exist
	nextPageId uint32
}

// newPager instantiates new pager for the given file. If the file exists,
// it opens the file and reads its metadata and checks invariants, otherwise
// it creates a new file and populates it with the metadata.
func newPager(path string, pageSize uint16) (*pager, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", path, err)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat the file %s: %w", path, err)
	}

	size := info.Size()
	if size == 0 {
		// initialize free pages block and metadata block
		p := &pager{file, pageSize, make(map[uint32]struct{}), 0}
		if err := initializeMetadata(p); err != nil {
			return nil, fmt.Errorf("failed to initialize metadata: %w", err)
		}

		if err := initializeFreePages(p); err != nil {
			return nil, fmt.Errorf("failed to initialize free pages: %w", err)
		}

		if err := p.flush(); err != nil {
			return nil, fmt.Errorf("failed to flush initialization changes: %w", err)
		}

		return p, nil
	}

	metadata, err := readMetadata(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	if metadata.pageSize != pageSize {
		return nil, fmt.Errorf("the file was created with page size %d, but given page size is %d", metadata.pageSize, pageSize)
	}

	freePages, err := readFreePages(file, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read free pages: %w", err)
	}

	used := (size - metadataSize)
	lastPageId := uint32(0)
	if used > 0 {
		lastPageId = uint32(used/int64(pageSize)) - 1
	}

	return &pager{file, pageSize, freePages, lastPageId}, nil
}

func initializeMetadata(p *pager) error {
	if _, err := p.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to the beginning of the file: %w", err)
	}

	data := encodeMetadata(&metadata{p.pageSize})
	if n, err := p.file.Write(data); err != nil {
		return fmt.Errorf("failed to write the metadata to the file: %w", err)
	} else if n < len(data) {
		return fmt.Errorf("failed to write all the data to the file, wrote %d bytes: %w", n, err)
	}

	return nil
}

func initializeFreePages(p *pager) error {
	pageId, err := p.new()
	if err != nil {
		return fmt.Errorf("failed to instantiate new page: %w", err)
	}

	if pageId != firstFreePageId {
		return fmt.Errorf("expected new page id to be %d for the new file, but got %d", firstFreePageId, pageId)
	}

	return nil
}

// readFreePages reads and initializes the list of free pages.
func readFreePages(f *os.File, pageSize uint16) (map[uint32]struct{}, error) {
	freePages := make(map[uint32]struct{})

	freePageId := firstFreePageId
	for freePageId != 0 {
		freePage, err := readFreePage(f, firstFreePageId, pageSize)
		if err != nil {
			return nil, fmt.Errorf("failed to read free page ")
		}

		for id, value := range freePage.freePages {
			freePages[id] = value
		}

		freePageId = freePage.nextPageId
	}

	return freePages, nil
}

func readFreePage(f *os.File, pageId uint32, pageSize uint16) (*freePage, error) {
	data, err := readPage(f, pageId, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageId, err)
	}

	freePage, err := decodeFreePage(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode free page: %w", err)
	}

	return freePage, nil
}

func decodeFreePage(data []byte) (*freePage, error) {
	pageIdNum := (len(data) - pageIdSize) / pageIdSize
	freePages := make(map[uint32]struct{})
	for i := 0; i < pageIdNum; i++ {
		from, to := i*pageIdSize, i*pageIdSize+pageIdSize
		pageId := decodeUint32(data[from:to])
		if pageId == 0 {
			break
		}

		freePages[pageId] = struct{}{}
	}

	nextPageId := decodeUint32(data[len(data)-pageIdSize:])

	return &freePage{freePages, nextPageId}, nil
}

// reads and decodes metadata from the specified file.
func readMetadata(f *os.File) (*metadata, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to the beginning of the file: %w", err)
	}

	data := make([]byte, metadataSize)
	if read, err := f.Read(data[:]); err != nil {
		return nil, fmt.Errorf("failed to read metadata from the file: %w", err)
	} else if read != metadataSize {
		return nil, fmt.Errorf("failed to read metadata from the file: read %d bytes, but must %d", read, metadataSize)
	}

	m, err := decodeMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}

	return m, nil
}

func encodeMetadata(m *metadata) []byte {
	data := make([]byte, metadataSize)

	d := encodeUint16(m.pageSize)
	copy(data[0:len(d)], d)

	return data
}

// decodes and returns metadata from the given byte slice.
func decodeMetadata(data []byte) (*metadata, error) {
	// the first block is the page size, encoded as uint32
	pageSize := decodeUint16(data[0:1])

	return &metadata{pageSize: pageSize}, nil
}

// newPage returns an identifier of the page that is free
// and can be used for write.
func (p *pager) new() (uint32, error) {
	if len(p.freePages) > 0 {
		for freePageId := range p.freePages {
			defer delete(p.freePages, freePageId)

			return freePageId, nil
		}
	}

	offset := int64((p.lastPageId)*uint32(p.pageSize)) + metadataSize
	if _, err := p.file.Seek(offset, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek to %d: %w", offset, err)
	}

	data := make([]byte, p.pageSize)
	if n, err := p.file.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write empty block: %w", err)
	} else if n < int(p.pageSize) {
		return 0, fmt.Errorf("failed to write all bytes of the empty block, wrote only %d bytes", n)
	}

	p.lastPageId++

	return p.lastPageId, nil
}

// free marks the page as free and the page can be reused.
func (p *pager) free(pageId uint32) error {
	p.freePages[pageId] = struct{}{}

	return nil
}

// read reads the page contents by the page identifier and returns
// its contents.
func (p *pager) read(pageId uint32) ([]byte, error) {
	// check that page is not removed (not in free list and exists)
	// pageId < current max page id && not in free list
	if pageId > p.lastPageId {
		return nil, fmt.Errorf("page %d does not exist", pageId)
	}

	return readPage(p.file, pageId, p.pageSize)
}

func readPage(f *os.File, pageId uint32, pageSize uint16) ([]byte, error) {
	offset := int64(metadataSize + pageId*uint32(pageSize))
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek at %d: %w", offset, err)
	}

	data := make([]byte, pageSize)
	if n, err := f.Read(data); err != nil {
		return nil, fmt.Errorf("failed to read the page data: %w", err)
	} else if n != int(pageSize) {
		return nil, fmt.Errorf("failed to read %d bytes, read %d", pageSize, n)
	}

	return data, nil
}

// write writes the page content.
func (p *pager) write(pageId uint32, data []byte) error {

	return nil
}

// truncate removes the free pages that are placed at the end of file.
func (p *pager) truncate() error {

	return nil
}

// flush flushes all the changes of the file to the persistent disk.
func (p *pager) flush() error {
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// close closes the pager and all underlying resources.
func (p *pager) close() error {
	if err := p.file.Close(); err != nil {
		return fmt.Errorf("failed to close the file: %w", err)
	}

	return nil
}
