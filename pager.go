package fbptree

import (
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
)

// for mocking the filesystem
var openFile = os.OpenFile

const minPageSize = 32
const maxPageSize = math.MaxUint16

// the size of the first metadata block in the file,
// reserved for different needs
const metadataSize = 1000
const customMetadataPosition = 500

// the id of the first free page
const firstFreePageId = uint32(1)
const pageIdSize = 4 // uint32

// pager is an abstaction over the file that represents the file
// as a set of pages. The file is splitten into
// the pages with the fixed size, usually 4096 bytes.
type pager struct {
	file     randomAccessFile
	pageSize uint16

	// id is any free page that can be used
	// and the value is free page container
	isFreePage map[uint32]*freePage
	// the pointer to the last free page
	lastFreePage *freePage

	// last page id is last created page id
	// it can be free or used - it does not matter
	lastPageId uint32

	freePages map[uint32]*freePage
	// key is the id of the page and the value is the id of the previous page
	prevPageIds map[uint32]uint32

	metadata *metadata
}

type metadata struct {
	pageSize uint16

	custom []byte
}

type freePage struct {
	pageId uint32
	ids    map[uint32]struct{}
	// 0 if does not exist
	nextPageId uint32
}

func (p *freePage) copy() *freePage {
	newIds := make(map[uint32]struct{})
	for key, value := range p.ids {
		newIds[key] = value
	}

	return &freePage{
		p.pageId,
		newIds,
		p.nextPageId,
	}
}

type randomAccessFile interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	Sync() error
	Stat() (fs.FileInfo, error)
	Truncate(size int64) error
}

// newPager instantiates new pager for the given file. If the file exists,
func openPager(path string, pageSize uint16) (*pager, error) {
	file, err := openFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", path, err)
	}

	pager, err := newPager(file, pageSize)
	if err != nil {
		file.Close()

		return nil, fmt.Errorf("failed to instantiate the pager: %w", err)
	}

	return pager, nil
}

// newPager instantiates new pager for the given file. If the file exists,
// it opens the file and reads its metadata and checks invariants, otherwise
// it creates a new file and populates it with the metadata.
func newPager(file randomAccessFile, pageSize uint16) (*pager, error) {
	if pageSize < minPageSize {
		return nil, fmt.Errorf("page size must be greater than or equal to %d", minPageSize)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat the file: %w", err)
	}

	size := info.Size()
	if size == 0 {
		// initialize free pages block and metadata block
		p := &pager{file, pageSize, make(map[uint32]*freePage), nil, 0, make(map[uint32]*freePage), make(map[uint32]uint32), &metadata{pageSize, nil}}
		if err := writeMetadata(p.file, p.metadata); err != nil {
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

	isFreePage, lastFreePage, freePages, prevPageIds, err := readFreePages(file, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read free pages: %w", err)
	}

	used := (size - metadataSize)
	lastPageId := uint32(0)
	if used > 0 {
		lastPageId = uint32(used / int64(pageSize))
	}

	return &pager{file, pageSize, isFreePage, lastFreePage, lastPageId, freePages, prevPageIds, metadata}, nil
}

func writeMetadata(w io.WriterAt, metadata *metadata) error {
	data := encodeMetadata(metadata)
	if n, err := w.WriteAt(data, 0); err != nil {
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

	ids := make(map[uint32]struct{})
	freePage := &freePage{pageId, ids, 0}
	p.lastFreePage = freePage
	p.freePages[pageId] = freePage

	return nil
}

// readFreePages reads and initializes the list of free pages.
func readFreePages(r io.ReaderAt, pageSize uint16) (map[uint32]*freePage, *freePage, map[uint32]*freePage, map[uint32]uint32, error) {
	isFreePage := make(map[uint32]*freePage)
	freePages := make(map[uint32]*freePage)
	prevPageIds := make(map[uint32]uint32)

	var prevPageId uint32
	freePageId := firstFreePageId
	var lastFreePage *freePage
	for freePageId != 0 {
		freePage, err := readFreePage(r, freePageId, pageSize)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("failed to read free page: %w", err)
		}

		for id := range freePage.ids {
			isFreePage[id] = freePage
		}
		freePages[freePageId] = freePage

		if prevPageId != 0 {
			prevPageIds[freePageId] = prevPageId
		}
		prevPageId = freePageId

		lastFreePage = freePage
		freePageId = freePage.nextPageId
	}

	return isFreePage, lastFreePage, freePages, prevPageIds, nil
}

func readFreePage(r io.ReaderAt, pageId uint32, pageSize uint16) (*freePage, error) {
	data, err := readPage(r, pageId, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageId, err)
	}

	freePage, err := decodeFreePage(pageId, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode free page: %w", err)
	}

	return freePage, nil
}

func decodeFreePage(pageId uint32, data []byte) (*freePage, error) {
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

	return &freePage{pageId, freePages, nextPageId}, nil
}

// reads and decodes metadata from the specified file.
func readMetadata(r io.ReaderAt) (*metadata, error) {
	data := make([]byte, metadataSize)
	if read, err := r.ReadAt(data[:], 0); err != nil {
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

	if len(m.custom) != 0 {
		s := encodeUint16(uint16(len(m.custom)))
		copy(data[customMetadataPosition:customMetadataPosition+len(s)], s)
		copy(data[customMetadataPosition+len(s):], m.custom)
	}

	return data
}

// decodes and returns metadata from the given byte slice.
func decodeMetadata(data []byte) (*metadata, error) {
	// the first block is the page size, encoded as uint16
	pageSize := decodeUint16(data[0:2])

	customMetadataSize := decodeUint16(data[customMetadataPosition : customMetadataPosition+2])
	var customMetadata []byte = nil
	if customMetadataSize != 0 {
		customMetadata = data[customMetadataPosition+2 : customMetadataPosition+2+customMetadataSize]
	}

	return &metadata{pageSize: pageSize, custom: customMetadata}, nil
}

// newPage returns an identifier of the page that is free
// and can be used for write.
func (p *pager) new() (uint32, error) {
	if len(p.isFreePage) > 0 {
		for freePageId := range p.isFreePage {
			freePage := p.isFreePage[freePageId]
			delete(freePage.ids, freePageId)

			data := encodeFreePage(freePage, p.pageSize)
			if err := writePage(p.file, freePage.pageId, data, p.pageSize); err != nil {
				freePage.ids[freePageId] = struct{}{}
				return 0, fmt.Errorf("failed to update the free page: %w", err)
			}

			delete(p.isFreePage, freePageId)

			return freePageId, nil
		}
	}

	offset := int64((p.lastPageId)*uint32(p.pageSize)) + metadataSize
	data := make([]byte, p.pageSize)
	if n, err := p.file.WriteAt(data, offset); err != nil {
		return 0, fmt.Errorf("failed to write empty block: %w", err)
	} else if n < int(p.pageSize) {
		return 0, fmt.Errorf("failed to write all bytes of the empty block, wrote only %d bytes", n)
	}

	p.lastPageId++

	return p.lastPageId, nil
}

// writeCustomMetadata writes custom metadata into the metadata section of the file.
func (p *pager) writeCustomMetadata(data []byte) error {
	maxCustomMetadataLen := (metadataSize - customMetadataPosition)
	if len(data) > maxCustomMetadataLen {
		return fmt.Errorf("custom metadata must be less than %d bytes", maxCustomMetadataLen)
	}

	p.metadata.custom = data

	err := writeMetadata(p.file, p.metadata)
	if err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// writeMetadata reads custom metadata from the metadata section of the file.
func (p *pager) readCustomMetadata() ([]byte, error) {
	metadata, err := readMetadata(p.file)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	return metadata.custom, nil
}

func (p *pager) isFree(pageId uint32) bool {
	_, isFreePage := p.isFreePage[pageId]

	return isFreePage
}

// free marks the page as free and the page can be reused.
func (p *pager) free(pageId uint32) error {
	if p.isFree(pageId) {
		return fmt.Errorf("the page is already free")
	}

	if (len(p.lastFreePage.ids)*pageIdSize + pageIdSize) < int(p.pageSize) {
		// update the page that contains the free pages
		p.lastFreePage.ids[pageId] = struct{}{}
		data := encodeFreePage(p.lastFreePage, p.pageSize)
		if err := writePage(p.file, p.lastFreePage.pageId, data, p.pageSize); err != nil {
			// revert the changes
			delete(p.lastFreePage.ids, pageId)

			return fmt.Errorf("failed to update the last free page: %w", err)
		}

		p.isFreePage[pageId] = p.lastFreePage
	} else {
		// if there is not enough space for the free page list
		newPageId, err := p.new()
		if err != nil {
			return fmt.Errorf("failed to instantiate new page: %w", err)
		}

		newIds := make(map[uint32]struct{})
		newIds[pageId] = struct{}{}
		newFreePage := &freePage{newPageId, newIds, 0}

		data := encodeFreePage(newFreePage, p.pageSize)
		if err := writePage(p.file, newPageId, data, p.pageSize); err != nil {
			return fmt.Errorf("failed to write the new free page: %w", err)
		}

		p.lastFreePage.nextPageId = newPageId
		data = encodeFreePage(p.lastFreePage, p.pageSize)
		if err := writePage(p.file, p.lastFreePage.pageId, data, p.pageSize); err != nil {
			// revert the changes
			p.lastFreePage.nextPageId = 0

			return fmt.Errorf("failed to update the last free page: %w", err)
		}

		p.prevPageIds[newPageId] = p.lastFreePage.pageId
		p.lastFreePage = newFreePage
		p.isFreePage[pageId] = newFreePage
		p.freePages[newPageId] = newFreePage
	}

	return nil
}

// encodeFreePage encodes free page identifiers into the chunks of byte slices.
func encodeFreePage(page *freePage, pageSize uint16) []byte {
	data := make([]byte, pageSize)
	copy(data[len(data)-pageIdSize:], encodeUint32(page.nextPageId))

	i := 0
	for freePageId := range page.ids {
		copy(data[i:], encodeUint32(freePageId))
		i += pageIdSize
	}

	return data
}

// read reads the page contents by the page identifier and returns
// its contents.
func (p *pager) read(pageId uint32) ([]byte, error) {
	if p.isFree(pageId) {
		return nil, fmt.Errorf("page %d does not exist or free", pageId)
	}

	return readPage(p.file, pageId, p.pageSize)
}

func writePage(w io.WriterAt, pageId uint32, data []byte, pageSize uint16) error {
	offset := int64(metadataSize + (pageId-1)*uint32(pageSize))

	if n, err := w.WriteAt(data, offset); err != nil {
		return fmt.Errorf("failed to write the page: %w", err)
	} else if n != len(data) {
		return fmt.Errorf("failed to write %d bytes, wrote %d", len(data), n)
	}

	return nil
}

func readPage(r io.ReaderAt, pageId uint32, pageSize uint16) ([]byte, error) {
	offset := int64(metadataSize + (pageId-1)*uint32(pageSize))
	data := make([]byte, pageSize)
	if n, err := r.ReadAt(data, offset); err != nil {
		return nil, fmt.Errorf("failed to read the page data: %w", err)
	} else if n != int(pageSize) {
		return nil, fmt.Errorf("failed to read %d bytes, read %d", pageSize, n)
	}

	return data, nil
}

// write writes the page content.
func (p *pager) write(pageId uint32, data []byte) error {
	if p.isFree(pageId) {
		return fmt.Errorf("page %d does not exist or free", pageId)
	}

	if len(data) != int(p.pageSize) {
		return fmt.Errorf("data length %d is greater than the page size %d", len(data), p.pageSize)
	}

	return writePage(p.file, pageId, data, p.pageSize)
}

// compact removes the free pages that are placed at the end of file and
// if the free page lists does not contains any free page, it frees the free page list.
func (p *pager) compact() error {
	newLastPageId := p.lastPageId
	removeFreePageIds := make([]uint32, 0)
	removeFreePages := make(map[uint32]*freePage)
	// the copy of free pages to be updated
	updateFreePages := make(map[uint32]*freePage)
	for pageId := p.lastPageId; pageId > firstFreePageId; pageId-- {
		if p.isFree(pageId) {
			removeFreePageIds = append(removeFreePageIds, pageId)

			freePage := p.isFreePage[pageId]
			updatePage, ok := updateFreePages[freePage.pageId]
			if !ok {
				updatePage = freePage.copy()
				updateFreePages[updatePage.pageId] = updatePage
			}
			delete(updatePage.ids, pageId)

			newLastPageId = pageId - 1
		} else if p.canDeleteFreePage(pageId) {
			freePage := p.freePages[pageId]
			removeFreePages[pageId] = freePage

			if prevPageId, ok := p.prevPageIds[pageId]; ok {
				prevPage := p.freePages[prevPageId]
				updatePage, ok := updateFreePages[prevPageId]
				if !ok {
					updatePage = prevPage.copy()
					updateFreePages[prevPageId] = updatePage
				}
				updatePage.nextPageId = freePage.nextPageId
			}

			newLastPageId = pageId - 1
		} else {
			break
		}
	}

	// update free pages and last free page id
	freeBytes := int64(len(removeFreePages)+len(removeFreePageIds)) * int64(p.pageSize)
	if freeBytes == 0 {
		return nil
	}

	stat, err := p.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get the file size: %w", err)
	}

	newSize := stat.Size() - freeBytes
	err = p.file.Truncate(newSize)
	if err != nil {
		return fmt.Errorf("failed to truncate the file: %w", err)
	}

	for pageId := range removeFreePages {
		delete(updateFreePages, pageId)
	}
	for pageId, updatePage := range updateFreePages {
		data := encodeFreePage(updatePage, p.pageSize)
		if err := writePage(p.file, pageId, data, p.pageSize); err != nil {
			return fmt.Errorf("failed to update the free page: %w", err)
		}
	}

	for pageId, updateFreePage := range updateFreePages {
		freePage := p.freePages[pageId]
		freePage.pageId = updateFreePage.pageId
		freePage.ids = updateFreePage.ids
		freePage.nextPageId = updateFreePage.nextPageId
	}
	for _, removeId := range removeFreePageIds {
		delete(p.isFreePage, removeId)
	}
	for pageId, removePage := range removeFreePages {
		if p.lastFreePage == removePage {
			p.lastFreePage = p.freePages[p.prevPageIds[removePage.pageId]]
		}

		delete(p.prevPageIds, pageId)
		delete(p.freePages, pageId)
	}

	p.lastPageId = newLastPageId

	return nil
}

// canDeleteFreePage checks if the page is a free page list container
// and if all the pages in the container are free.
func (p *pager) canDeleteFreePage(pageId uint32) bool {
	freePage, isFreePage := p.freePages[pageId]
	if !isFreePage {
		return false
	}

	for id := range freePage.ids {
		if _, isFree := p.isFreePage[id]; !isFree {
			return false
		}
	}

	return true
}

// flush flushes all the changes of the file to the persistent disk.
func (p *pager) flush() error {
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// close flushes the changes and closes all underlying resources.
func (p *pager) close() error {
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	if err := p.file.Close(); err != nil {
		return fmt.Errorf("failed to close the file: %w", err)
	}

	return nil
}
