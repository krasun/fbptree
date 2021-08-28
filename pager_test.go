package fbptree

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestNewPagerInitializesProperly(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	if len(p.isFreePage) != 0 {
		t.Fatalf("expected free pages size is 0, but got %d", len(p.isFreePage))
	}

	if p.lastPageId != firstFreePageId {
		t.Fatalf("expected last page id == 1, but got %d", p.lastPageId)
	}

	if p.pageSize != 4096 {
		t.Fatalf("expected page size to be %d, but got %d", 4006, p.pageSize)
	}
}

func TestNewPage(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	newPageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	if newPageId <= firstFreePageId {
		t.Fatalf("new page id must be >= %d:", firstFreePageId)
	}

	_, exists := p.isFreePage[newPageId]
	if exists {
		t.Fatalf("new page id must not be in the free page list")
	}

	stat, err := p.file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %s", err)
	}

	// metadata + free page + new page
	expectedSize := metadataSize + 4096*2
	if stat.Size() != int64(expectedSize) {
		t.Fatalf("expected file size %d, but got %d", expectedSize, stat.Size())
	}
}

func TestDeleteFreeSparseFile(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	freePageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	_, err = p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	err = p.free(freePageId)
	if err != nil {
		t.Fatalf("failed to free page: %s", err)
	}

	_, exists := p.isFreePage[freePageId]
	if !exists {
		t.Fatalf("new page id must be in the free page list")
	}

	stat, err := p.file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %s", err)
	}

	// metadata + free page + 2 new pages, but the file is sparse now
	expectedSize := metadataSize + 4096*3
	if stat.Size() != int64(expectedSize) {
		t.Fatalf("expected file size %d, but got %d", expectedSize, stat.Size())
	}
}

func TestDeleteFree(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	_, err = p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	freePageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	_, err = p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	err = p.free(freePageId)
	if err != nil {
		t.Fatalf("failed to free page: %s", err)
	}

	if !p.isFree(freePageId) {
		t.Fatalf("new page id must be in the free page list")
	}

	stat, err := p.file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %s", err)
	}

	// metadata + free page + 3 new pages, but the file is sparse now
	expectedSize := metadataSize + 4096*4
	if stat.Size() != int64(expectedSize) {
		t.Fatalf("expected file size %d, but got %d", expectedSize, stat.Size())
	}

	p.close()

	p, err = openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	if !p.isFree(freePageId) {
		t.Fatalf("new page id must be in the free page list")
	}
}

func TestNewAfterFreeUsesFreePage(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	freePageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	err = p.free(freePageId)
	if err != nil {
		t.Fatalf("failed to free page: %s", err)
	}

	newPageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	if newPageId != freePageId {
		t.Fatalf("new page id must be equal to free page id %d, but got %d", freePageId, newPageId)
	}

	_, exists := p.isFreePage[newPageId]
	if exists {
		t.Fatalf("new page id must not be in the free page list")
	}

	stat, err := p.file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %s", err)
	}

	// metadata + free page + 1 new page
	expectedSize := metadataSize + 4096*2
	if stat.Size() != int64(expectedSize) {
		t.Fatalf("expected file size %d, but got %d", expectedSize, stat.Size())
	}
}

func TestFreePageSplitting(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	var pageSize uint16 = 4096
	p, err := openPager(path.Join(dbDir, "test.db"), pageSize)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	iterations := int((pageSize / pageIdSize) + 1)
	ids := make([]uint32, 0)
	for i := 0; i <= iterations; i++ {
		freePageId, err := p.new()
		if err != nil {
			t.Fatalf("failed to new page: %s", err)
		}

		ids = append(ids, freePageId)
	}

	var lastFreePageId uint32
	for _, freePageId := range ids {
		err = p.free(freePageId)
		if err != nil {
			t.Fatalf("failed to free page: %s", err)
		}

		lastFreePageId = freePageId
	}

	stat, err := p.file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %s", err)
	}

	// metadata + iterations + 2 free pages
	expectedSize := metadataSize + 4096*(iterations+2)
	if stat.Size() != int64(expectedSize) {
		t.Fatalf("expected file size %d, but got %d", expectedSize, stat.Size())
	}

	p.close()

	p, err = openPager(path.Join(dbDir, "test.db"), pageSize)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	if !p.isFree(lastFreePageId) {
		t.Fatalf("new page id must be in the free page list")
	}
}

func TestReadAndWrite(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	newPageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	var writtenData [4096]byte
	// some random data
	writtenData[0] = 1
	writtenData[2] = 3
	writtenData[1023] = 10
	writtenData[2034] = 0xAE

	err = p.write(newPageId, writtenData[:])
	if err != nil {
		t.Fatalf("failed to write the page: %s", err)
	}

	stat, err := p.file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %s", err)
	}

	// metadata + free page + new page
	expectedSize := metadataSize + 4096*2
	if stat.Size() != int64(expectedSize) {
		t.Fatalf("expected file size %d, but got %d", expectedSize, stat.Size())
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close the pager: %s", err)
	}

	p, err = openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	readData, err := p.read(newPageId)
	if err != nil {
		t.Fatalf("failed to read the data: %s", err)
	}

	if !bytes.Equal(writtenData[:], readData) {
		t.Fatalf("the written data is not equal to the read data")
	}
}

func TestReadNonExistentPageError(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	_, err = p.read(10)
	if err == nil {
		t.Fatal("must return an error for nonexistent page")
	}
}

func TestReadFreePageError(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	newPageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to instantiate new page: %s", err)
	}
	err = p.free(newPageId)
	if err != nil {
		t.Fatalf("failed to free new page: %s", err)
	}

	_, err = p.read(newPageId)
	if err == nil {
		t.Fatal("must return an error for free page")
	}
}

func TestCreatedWithDifferentPageSize(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	_, err = openPager(path.Join(dbDir, "test.db"), 2000)
	if err == nil {
		t.Fatal("must return an error for the different page size")
	}
}

func TestReadPageInTruncatedFileError(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	newPageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to instantiate new page: %s", err)
	}

	var data [4096]byte
	// some random data
	data[0] = 10
	data[2] = 30
	data[3017] = 25

	err = p.write(newPageId, data[:])
	if err != nil {
		t.Fatalf("failed to write the page: %s", err)
	}

	// truncate file
	f, err := os.OpenFile(path.Join(dbDir, "test.db"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("failed to open the file: %s", err)
	}

	info, err := f.Stat()
	if err != nil {
		t.Fatalf("failed to stat the file: %s", err)
	}

	err = f.Truncate(info.Size() - 1)
	if err != nil {
		t.Fatalf("failed to truncate the file: %s", err)
	}

	f.Close()

	_, err = p.read(newPageId)
	if err == nil {
		t.Fatal("must return an error for reading page in the truncated file")
	}
}

func TestFreeAlreadyFreePageError(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	freePageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	err = p.free(freePageId)
	if err != nil {
		t.Fatalf("failed to free page: %s", err)
	}

	err = p.free(freePageId)
	if err == nil {
		t.Fatal("must return an error for freeing the same page twice")
	}
}

func TestWriteToFreePageError(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	newPageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to instantiate new page: %s", err)
	}

	err = p.free(newPageId)
	if err != nil {
		t.Fatalf("failed to free page: %s", err)
	}

	var data [4096]byte
	// some random data
	data[0] = 10
	data[2] = 30
	data[3017] = 25

	err = p.write(newPageId, data[:])
	if err == nil {
		t.Fatal("must return an error for writing into the free page")
	}
}

func TestOpenPagerReturnsAnError(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	prevOpenFileFunc := openFile
	defer func() {
		if r := recover(); r == nil {
			openFile = prevOpenFileFunc
		}
	}()

	openFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, fmt.Errorf("some error")
	}

	_, err := openPager(path.Join(dbDir, "test.db"), 4096)
	openFile = prevOpenFileFunc

	if err == nil {
		t.Fatal("must return the error for opening file with error")
	}
}

func TestErrorOnStat(t *testing.T) {
	mockedFile := newMockedFile()
	mockedFile.setErrorOnStat(fmt.Errorf("some error"))

	_, err := newPager(mockedFile, 4096)
	if err == nil {
		t.Fatal("must return the error for stat")
	}
}

func TestCompactFreesAllPagesAndFreePageListItself(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	var pageSize uint16 = 4096
	p, err := openPager(path.Join(dbDir, "test.db"), pageSize)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	iterations := int((pageSize / pageIdSize) + 1)
	ids := make([]uint32, 0)
	for i := 0; i <= iterations; i++ {
		freePageId, err := p.new()
		if err != nil {
			t.Fatalf("failed to new page: %s", err)
		}

		ids = append(ids, freePageId)
	}

	for _, freePageId := range ids {
		err = p.free(freePageId)
		if err != nil {
			t.Fatalf("failed to free page: %s", err)
		}
	}

	stat, err := p.file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %s", err)
	}

	// metadata + iterations + 2 free pages
	expectedSize := metadataSize + 4096*(iterations+2)
	if stat.Size() != int64(expectedSize) {
		t.Fatalf("expected file size %d, but got %d", expectedSize, stat.Size())
	}

	p.close()

	p, err = openPager(path.Join(dbDir, "test.db"), pageSize)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	err = p.compact()
	if err != nil {
		t.Fatalf("failed to compact: %s", err)
	}

	err = p.flush()
	if err != nil {
		t.Fatalf("failed to flush: %s", err)
	}

	stat, err = p.file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %s", err)
	}

	// metadata + 1 free page container
	expectedSize = metadataSize + int(pageSize)
	if stat.Size() != int64(expectedSize) {
		t.Fatalf("expected file size %d, but got %d", expectedSize, stat.Size())
	}
}

func TestCompactReadWriteAfterCompact(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	var pageSize uint16 = 4096
	p, err := openPager(path.Join(dbDir, "test.db"), pageSize)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	iterations := int((pageSize / pageIdSize) + 1)
	ids := make([]uint32, 0)
	for i := 0; i <= iterations; i++ {
		freePageId, err := p.new()
		if err != nil {
			t.Fatalf("failed to new page: %s", err)
		}

		ids = append(ids, freePageId)
	}

	for _, freePageId := range ids {
		err = p.free(freePageId)
		if err != nil {
			t.Fatalf("failed to free page: %s", err)
		}
	}

	stat, err := p.file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %s", err)
	}

	// metadata + iterations + 2 free pages
	expectedSize := metadataSize + int(pageSize)*(iterations+2)
	if stat.Size() != int64(expectedSize) {
		t.Fatalf("expected file size %d, but got %d", expectedSize, stat.Size())
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close: %s", err)
	}

	p, err = openPager(path.Join(dbDir, "test.db"), pageSize)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	err = p.compact()
	if err != nil {
		t.Fatalf("failed to compact: %s", err)
	}

	err = p.flush()
	if err != nil {
		t.Fatalf("failed to flush: %s", err)
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close: %s", err)
	}

	p, err = openPager(path.Join(dbDir, "test.db"), pageSize)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	newPageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	var writtenData [4096]byte
	// some random data
	writtenData[0] = 1
	writtenData[2] = 3
	writtenData[1023] = 10
	writtenData[2034] = 0xAE

	err = p.write(newPageId, writtenData[:])
	if err != nil {
		t.Fatalf("failed to write the page: %s", err)
	}

	stat, err = p.file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %s", err)
	}

	// metadata + free page + new page
	expectedSize = metadataSize + int(pageSize)*2
	if stat.Size() != int64(expectedSize) {
		t.Fatalf("expected file size %d, but got %d", expectedSize, stat.Size())
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close the pager: %s", err)
	}

	p, err = openPager(path.Join(dbDir, "test.db"), pageSize)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	readData, err := p.read(newPageId)
	if err != nil {
		t.Fatalf("failed to read the data: %s", err)
	}

	if !bytes.Equal(writtenData[:], readData) {
		t.Fatalf("the written data is not equal to the read data")
	}
}

type mockedFile struct {
	randomAccessFile

	errorOnStat error
}

func newMockedFile() *mockedFile {
	return new(mockedFile)
}

func (f *mockedFile) setErrorOnStat(errorOnStat error) {
	f.errorOnStat = errorOnStat
}

func (f *mockedFile) Stat() (os.FileInfo, error) {
	return nil, f.errorOnStat
}
