package fbptree

import (
	"fmt"
	"math"
)

const maxRecordSize = math.MaxUint32

// records is an abstraction over the pages that
// allows to gather pages into the records of the variable size.
type records struct {
	pager *pager
}

// newRecords instantiates new instance of the records.
func newRecords(pager *pager) *records {
	return &records{pager}
}

// firstRecordId the identifier of the first record that can be used
// to store data.
// The record itself is not necessarily free or used, the idea of this function
// is to allow to store some initial data for the application and have consistent pointer
// to this data.
func (r *records) firstRecordId() uint32 {
	return r.pager.firstPageId()
}

// new instantiates new record and returns its identifier or error.
func (r *records) new() (uint32, error) {
	newPageId, err := r.pager.new()
	if err != nil {
		return 0, fmt.Errorf("failed to instantiate the first block page: %w", err)
	}

	return newPageId, nil
}

// write writes record and accepts variable data length, in case if data
// length is larger than page size, it will require more pages and update them.
func (r *records) write(recordId uint32, data []byte) error {
	recordSize := len(data)
	if recordSize >= maxRecordSize {
		return fmt.Errorf("the record size must be less than %d", maxRecordSize)
	}

	pageData, err := r.pager.read(recordId)
	if err != nil {
		return fmt.Errorf("failed to read the initial record page %d: %w", recordId, err)
	}
	nextId := nextRecordId(pageData)

	freeNextPage := true
	writeSize := recordSize
	if recordSize > (len(pageData) - 16) {
		freeNextPage = false
		writeSize = len(pageData) - 16
	}
	written := writeSize

	if freeNextPage {
		clearNextRecordId(pageData)
	}

	copy(pageData[8:16], encodeUint32(uint32(recordSize)))
	copy(pageData[16:], data[0:writeSize])

	var newPageId uint32
	if nextId == 0 && written < recordSize {
		newPageId, err = r.pager.new()
		if err != nil {
			return fmt.Errorf("failed to initialize new page: %w", err)
		}

		setNextRecordId(pageData, newPageId)
	}

	if err := r.pager.write(recordId, pageData); err != nil {
		return fmt.Errorf("failed to write the page data for page %d: %w", recordId, err)
	}

	for nextId != 0 {
		pageId := nextId
		pageData, err := r.pager.read(pageId)
		if err != nil {
			return fmt.Errorf("failed to read page %d: %w", nextId, err)
		}

		nextId = nextRecordId(pageData)
		if freeNextPage {
			if err := r.pager.free(pageId); err != nil {
				return fmt.Errorf("failed to free page %d: %w", pageId, err)
			}

			continue
		}

		if written < recordSize {
			toWrite := recordSize - written
			if toWrite > (len(pageData) - 8) {
				toWrite = len(pageData) - 8
			}

			copy(pageData[8:], data[written:written+toWrite])
			written += toWrite
		}

		freeNextPage = written >= recordSize
		if freeNextPage {
			clearNextRecordId(pageData)
		}

		if nextId == 0 && written < recordSize {
			newPageId, err = r.pager.new()
			if err != nil {
				return fmt.Errorf("failed to initialize new page: %w", err)
			}

			setNextRecordId(pageData, newPageId)
		}

		if err := r.pager.write(pageId, pageData); err != nil {
			return fmt.Errorf("failed to write page %d: %w", pageId, err)
		}
	}

	for written < recordSize {
		pageId := newPageId
		pageData := make([]byte, r.pager.pageSize)

		toWrite := recordSize - written
		if toWrite > (len(pageData) - 8) {
			toWrite = len(pageData) - 8
		}

		copy(pageData[8:], data[written:written+toWrite])
		written += toWrite

		if written < recordSize {
			newPageId, err = r.pager.new()
			if err != nil {
				return fmt.Errorf("failed to initialize new page: %w", err)
			}

			setNextRecordId(pageData, newPageId)
		}

		if err := r.pager.write(pageId, pageData); err != nil {
			return fmt.Errorf("failed to write page %d: %w", newPageId, err)
		}
	}

	return nil
}

func reset(data []byte) {
	for i := 0; i < len(data); i++ {
		data[i] = 0
	}
}

// Free frees all pages used by the record.
func (r *records) free(recordId uint32) error {
	data, err := r.pager.read(recordId)
	if err != nil {
		return fmt.Errorf("failed to read initial record page: %w", err)
	}

	for nextId := recordId; nextId != 0; nextId = nextRecordId(data) {
		data, err = r.pager.read(nextId)
		if err != nil {
			return fmt.Errorf("failed to read page %d: %w", nextId, err)
		}

		err = r.pager.free(nextId)
		if err != nil {
			return fmt.Errorf("failed to free page %d: %w", nextId, err)
		}
	}

	return nil
}

// read reads all the data in the record pages and returns it. It is not aligned
// to the page size.
func (r *records) read(recordId uint32) ([]byte, error) {
	data, err := r.pager.read(recordId)
	if err != nil {
		return nil, fmt.Errorf("failed to read initial record page: %w", err)
	}

	recordData := make([]byte, recordSize(data))
	copy(recordData, data[16:])
	for nextId, pageCount := nextRecordId(data), 1; nextId != 0; nextId, pageCount = nextRecordId(data), pageCount+1 {
		data, err = r.pager.read(nextId)
		if err != nil {
			return nil, fmt.Errorf("failed to read page %d: %w", nextId, err)
		}

		from := pageCount*(int(r.pager.pageSize)-8) - 8
		copy(recordData[from:], data[8:])
	}

	return recordData, nil
}

func setNextRecordId(pageData []byte, nextId uint32) {
	copy(pageData[0:8], encodeUint32(nextId))
}

func clearNextRecordId(pageData []byte) {
	reset(pageData[0:8])
}

func recordSize(pageData []byte) uint32 {
	return decodeUint32(pageData[8:16])
}

func nextRecordId(pageData []byte) uint32 {
	return decodeUint32(pageData[0:8])
}
