package fbptree

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestWriteLargerThanOnePageWithNewPages(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	r := newRecords(p)
	newRecordId, err := r.new()
	if err != nil {
		t.Fatalf("failed to new record: %s", err)
	}

	writeData := make([]byte, 100)
	for i := 0; i < len(writeData); i++ {
		writeData[i] = byte(i % 256)
	}

	err = r.write(newRecordId, writeData)
	if err != nil {
		t.Fatalf("failed to write the record: %s", err)
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close the pager: %s", err)
	}

	p, err = openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	r = newRecords(p)
	readData, err := r.read(newRecordId)
	if err != nil {
		t.Fatalf("failed to read the data: %s", err)
	}

	if !bytes.Equal(writeData, readData) {
		t.Fatalf("the written data is not equal to the read data")
	}
}

func TestFreeLargerThanOnePage(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	r := newRecords(p)
	newRecordId, err := r.new()
	if err != nil {
		t.Fatalf("failed to new record: %s", err)
	}

	writeData := make([]byte, 100)
	for i := 0; i < len(writeData); i++ {
		writeData[i] = byte(i % 256)
	}

	err = r.write(newRecordId, writeData)
	if err != nil {
		t.Fatalf("failed to write the record: %s", err)
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close the pager: %s", err)
	}

	p, err = openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	r = newRecords(p)

	err = r.free(newRecordId)
	if err != nil {
		t.Fatalf("failed to free the record: %s", err)
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close the pager: %s", err)
	}

	p, err = openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	if len(p.isFreePage) < 5 {
		t.Fatalf("must have at least 3 pages, but has %d", len(p.isFreePage))
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close the pager: %s", err)
	}
}

func TestWriteLargerThanOnePageRewritesWithLargerData(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	r := newRecords(p)
	newRecordId, err := r.new()
	if err != nil {
		t.Fatalf("failed to new record: %s", err)
	}

	writeData := make([]byte, 100)
	for i := 0; i < len(writeData); i++ {
		writeData[i] = byte(i % 200)
	}

	err = r.write(newRecordId, writeData)
	if err != nil {
		t.Fatalf("failed to record the page: %s", err)
	}

	writeData = make([]byte, 200)
	for i := 0; i < len(writeData); i++ {
		writeData[i] = byte((i + 1) % 150)
	}

	err = r.write(newRecordId, writeData)
	if err != nil {
		t.Fatalf("failed to record the page: %s", err)
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close the pager: %s", err)
	}

	p, err = openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	r = newRecords(p)
	readData, err := r.read(newRecordId)
	if err != nil {
		t.Fatalf("failed to read the data: %s", err)
	}

	if !bytes.Equal(writeData, readData) {
		t.Fatalf("the written data is not equal to the read data")
	}
}

func TestWriteLargerThanOnePageRewritesWithLessData(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	r := newRecords(p)
	newRecordId, err := r.new()
	if err != nil {
		t.Fatalf("failed to new record: %s", err)
	}

	writeData := make([]byte, 200)
	for i := 0; i < len(writeData); i++ {
		writeData[i] = byte(i % 200)
	}

	err = r.write(newRecordId, writeData)
	if err != nil {
		t.Fatalf("failed to record the page: %s", err)
	}

	writeData = make([]byte, 100)
	for i := 0; i < len(writeData); i++ {
		writeData[i] = byte((i + 1) % 150)
	}

	err = r.write(newRecordId, writeData)
	if err != nil {
		t.Fatalf("failed to record the page: %s", err)
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close the pager: %s", err)
	}

	p, err = openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	r = newRecords(p)
	readData, err := r.read(newRecordId)
	if err != nil {
		t.Fatalf("failed to read the data: %s", err)
	}

	if !bytes.Equal(writeData, readData) {
		t.Fatalf("the written data is not equal to the read data")
	}
}

func TestWriteTwoPagesAndRewriteWithOnePage(t *testing.T) {
	dbDir, _ := ioutil.TempDir(os.TempDir(), "example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	p, err := openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}
	defer p.close()

	r := newRecords(p)
	newRecordId, err := r.new()
	if err != nil {
		t.Fatalf("failed to new record: %s", err)
	}

	writeData := make([]byte, 40)
	for i := 0; i < len(writeData); i++ {
		writeData[i] = byte(i % 200)
	}

	err = r.write(newRecordId, writeData)
	if err != nil {
		t.Fatalf("failed to record the page: %s", err)
	}

	writeData = make([]byte, 10)
	for i := 0; i < len(writeData); i++ {
		writeData[i] = byte((i + 1) % 150)
	}

	err = r.write(newRecordId, writeData)
	if err != nil {
		t.Fatalf("failed to write the record: %s", err)
	}

	err = p.close()
	if err != nil {
		t.Fatalf("failed to close the pager: %s", err)
	}

	p, err = openPager(path.Join(dbDir, "test.db"), 32)
	if err != nil {
		t.Fatalf("failed to initialize the pager: %s", err)
	}

	r = newRecords(p)
	readData, err := r.read(newRecordId)
	if err != nil {
		t.Fatalf("failed to read the data: %s", err)
	}

	if !bytes.Equal(writeData, readData) {
		t.Fatalf("the written data is not equal to the read data")
	}
}
