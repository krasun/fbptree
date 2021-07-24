package fbptree

import (
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

	p, err := newPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	if len(p.freePages) != 0 {
		t.Fatalf("expected free pages size is 0, but got %d", len(p.freePages))
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

	p, err := newPager(path.Join(dbDir, "test.db"), 4096)
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

	_, exists := p.freePages[newPageId]
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

	p, err := newPager(path.Join(dbDir, "test.db"), 4096)
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

	_, exists := p.freePages[freePageId]
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

	p, err := newPager(path.Join(dbDir, "test.db"), 4096)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	_, err = p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	freePageId, err := p.new()
	if err != nil {
		t.Fatalf("failed to new page: %s", err)
	}

	err = p.free(freePageId)
	if err != nil {
		t.Fatalf("failed to free page: %s", err)
	}

	_, exists := p.freePages[freePageId]
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

func TestNewAfterFreeUsesFreePage(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := newPager(path.Join(dbDir, "test.db"), 4096)
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

	_, exists := p.freePages[newPageId]
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

func TestFreeListWithMoreThanOneFreePage(t *testing.T) {

}
