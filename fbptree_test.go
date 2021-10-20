package fbptree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"reflect"
	"sort"
	"time"

	"testing"
)

func Example() {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")

	tree, err := Open(dbPath, PageSize(4096), Order(500))
	if err != nil {
		panic(fmt.Errorf("failed to open B+ tree %s: %w", dbDir, err))
	}

	_, _, err = tree.Put([]byte("Hi!"), []byte("Hello world, B+ tree!"))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	_, _, err = tree.Put([]byte("Does it override key?"), []byte("No!"))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	_, _, err = tree.Put([]byte("Does it override key?"), []byte("Yes, absolutely! The key has been overridden."))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	if err := tree.Close(); err != nil {
		panic(fmt.Errorf("failed to close: %w", err))
	}

	tree, err = Open(dbPath, PageSize(4096), Order(500))
	if err != nil {
		panic(fmt.Errorf("failed to open B+ tree %s: %w", dbDir, err))
	}

	value, ok, err := tree.Get([]byte("Hi!"))
	if err != nil {
		panic(fmt.Errorf("failed to get value: %w", err))
	}
	if !ok {
		fmt.Println("failed to find value")
	}

	fmt.Println(string(value))

	value, ok, err = tree.Get([]byte("Does it override key?"))
	if err != nil {
		panic(fmt.Errorf("failed to get value: %w", err))
	}
	if !ok {
		fmt.Println("failed to find value")
	}

	if err := tree.Close(); err != nil {
		panic(fmt.Errorf("failed to close: %w", err))
	}

	fmt.Println(string(value))
	// Output:
	// Hello world, B+ tree!
	// Yes, absolutely! The key has been overridden.
}

func TestOrderError(t *testing.T) {
	_, err := Open("somepath", Order(2))
	if err == nil {
		t.Fatal("must return an error, but it does not")
	}
}

var treeCases = []struct {
	key   byte
	value string
}{
	{11, "11"},
	{18, "18"},
	{7, "7"},
	{15, "15"},
	{0, "0"},
	{16, "16"},
	{14, "14"},
	{33, "33"},
	{25, "25"},
	{42, "42"},
	{60, "60"},
	{2, "2"},
	{1, "1"},
	{74, "74"},
}

func TestNew(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")

	tree, _ := Open(dbPath)
	if tree == nil {
		t.Fatal("expected new *BPTree instance, but got nil")
	}
}

func TestPutAndGet(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	for order := 3; order <= 7; order++ {
		dbPath := path.Join(dbDir, fmt.Sprintf("sample_%d.data", order))

		tree, err := Open(dbPath, PageSize(4096), Order(order))
		if err != nil {
			t.Fatalf("failed to open B+ tree %s: %s", dbDir, err)
		}

		for _, c := range treeCases {
			prev, exists, err := tree.Put([]byte{c.key}, []byte(c.value))
			if err != nil {
				t.Fatalf("failed to put key %v: %s", c.key, err)
			}
			if prev != nil {
				t.Fatalf("the key already exists %v", c.key)
			}
			if exists {
				t.Fatalf("the key already exists %v", c.key)
			}
		}

		if err := tree.Close(); err != nil {
			t.Fatalf("failed to close: %s", err)
		}

		tree, err = Open(dbPath, PageSize(4096), Order(order))
		if err != nil {
			panic(fmt.Errorf("failed to open B+ tree %s: %w", dbDir, err))
		}

		for _, c := range treeCases {
			value, ok, err := tree.Get([]byte{c.key})
			if err != nil {
				t.Fatalf("failed to get key %v: %s", c.key, err)
			}
			if !ok {
				t.Fatalf("failed to get value by key %d", c.key)
			}

			if string(value) != c.value {
				t.Fatalf("expected to get value %s fo key %d, but got %s", c.value, c.key, string(value))
			}
		}
	}
}

func TestNil(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")

	tree, _ := Open(dbPath)
	if tree == nil {
		t.Fatal("expected new *BPTree instance, but got nil")
	}

	tree.Put(nil, []byte{1})

	_, ok, _ := tree.Get(nil)
	if !ok {
		t.Fatalf("key nil is not found")
	}
}

func TestPutOverrides(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")

	tree, _ := Open(dbPath)
	if tree == nil {
		t.Fatal("expected new *BPTree instance, but got nil")
	}

	prev, exists, err := tree.Put([]byte{1}, []byte{1})
	if err != nil {
		t.Fatalf("failed to put key: %s", err)
	}
	if prev != nil {
		t.Fatal("previous value must be nil for the new key")
	}
	if exists {
		t.Fatal("previous value must be nil for the new key")
	}

	prev, exists, err = tree.Put([]byte{1}, []byte{2})
	if err != nil {
		t.Fatalf("failed to put key: %s", err)
	}
	if !bytes.Equal(prev, []byte{1}) {
		t.Fatalf("previous value must be %v, but got %v", []byte{1}, prev)
	}
	if !exists {
		t.Fatalf("exists must be true for key %v", []byte{1})
	}

	value, ok, err := tree.Get([]byte{1})
	if err != nil {
		t.Fatalf("failed to get key: %s", err)
	}
	if !ok {
		t.Fatalf("key %d is not found, but must be overridden", 1)
	}

	if !bytes.Equal(value, []byte{2}) {
		t.Fatalf("key %d is not overridden", 1)
	}
}

func TestGetForNonExistentValue(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")

	tree, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open tree: %s", err)
	}

	for _, c := range treeCases {
		tree.Put([]byte{c.key}, []byte(c.value))
	}

	value, ok, err := tree.Get([]byte{230})
	if err != nil {
		t.Fatalf("failed to get key: %s", err)
	}
	if value != nil {
		t.Fatalf("expected value to be nil, but got %s", value)
	}
	if ok {
		t.Fatalf("expected ok to be false, but got %v", ok)
	}
}

func TestGetForEmptyTree(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")
	tree, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open tree: %s", err)
	}

	value, ok, err := tree.Get([]byte{1})
	if err != nil {
		t.Fatalf("failed to get key: %s", err)
	}
	if value != nil {
		t.Fatalf("expected value to be nil, but got %s", value)
	}
	if ok {
		t.Fatalf("expected ok to be false, but got %v", ok)
	}
}

func TestForEach(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")
	tree, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open tree: %s", err)
	}

	for _, c := range treeCases {
		tree.Put([]byte{c.key}, []byte(c.value))
	}

	actual := make([]byte, 0)
	tree.ForEach(func(key []byte, value []byte) {
		actual = append(actual, key...)
	})

	isSorted := sort.SliceIsSorted(actual, func(i, j int) bool {
		return actual[i] < actual[j]
	})
	if !isSorted {
		t.Fatalf("each does not traverse in sorted order, produced result: %s", actual)
	}

	expected := make([]byte, 0)
	for _, c := range treeCases {
		expected = append(expected, c.key)
	}
	sort.Slice(expected, func(i, j int) bool {
		return expected[i] < expected[j]
	})

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("%v != %v", expected, actual)
	}
}

func TestForEachForEmptyTree(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")
	tree, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open tree: %s", err)
	}

	tree.ForEach(func(key []byte, value []byte) {
		t.Fatal("call is not expected")
	})
}

func TestKeyOrder(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")
	tree, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open tree: %s", err)
	}

	for _, c := range treeCases {
		tree.Put([]byte{c.key}, []byte(c.value))
	}

	keys := make([]byte, 0)
	tree.ForEach(func(key, value []byte) {
		keys = append(keys, key[0])
	})

	isSorted := sort.SliceIsSorted(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	if len(keys) == 0 {
		t.Fatal("keys are empty")
	}
	if !isSorted {
		t.Fatal("keys are empty keys are not sorted")
	}
}

func TestPutAndGetRandomized(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	size := 10000
	keys := r.Perm(size)

	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	for order := 3; order <= 7; order++ {
		dbPath := path.Join(dbDir, fmt.Sprintf("sample_%d.data", order))
		tree, err := Open(dbPath, Order(order))
		if err != nil {
			t.Fatalf("failed to open tree: %s", err)
		}

		for i, k := range keys {
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))
			value := make([]byte, 4)
			binary.LittleEndian.PutUint32(value, uint32(i))

			prev, exists, _ := tree.Put(key, value)
			if prev != nil {
				t.Fatalf("the key already exists %v", k)
			}
			if exists {
				t.Fatalf("the key already exists %v", k)
			}
		}
		tree.Close()

		tree, err = Open(dbPath, Order(order))
		if err != nil {
			t.Fatalf("failed to open tree: %s", err)
		}

		for i, k := range keys {
			expectedValue := uint32(i)
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))

			v, ok, _ := tree.Get(key)
			if !ok {
				t.Fatalf("failed to get value by key %d, tree size = %d, order = %d", k, tree.Size(), order)
			}

			actualValue := binary.LittleEndian.Uint32(v)
			if expectedValue != actualValue {
				t.Fatalf("expected to get value %d fo key %d, but got %d", expectedValue, k, actualValue)
			}
		}
	}
}

func TestPutAndDeleteRandomized(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	size := 1000
	keys := r.Perm(size)

	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	for order := 3; order <= 7; order++ {
		dbPath := path.Join(dbDir, fmt.Sprintf("sample_%d.data", order))
		tree, _ := Open(dbPath, Order(order))
		if err != nil {
			t.Fatalf("failed to open tree: %s", err)
		}

		for i, k := range keys {
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))
			value := make([]byte, 4)
			binary.LittleEndian.PutUint32(value, uint32(i))

			prev, exists, _ := tree.Put(key, value)
			if prev != nil {
				t.Fatalf("the key already exists %v", k)
			}
			if exists {
				t.Fatalf("the key already exists %v", k)
			}
		}

		tree.Close()

		tree, err := Open(dbPath, Order(order))
		if err != nil {
			t.Fatalf("failed to open tree: %s", err)
		}

		for i, k := range keys {
			expectedValue := uint32(i)
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))

			v, ok, err := tree.Delete(key)
			if err != nil {
				t.Fatalf("failed to delete value by key %d, tree size = %d, order = %d: %s", k, tree.Size(), order, err)
			}

			if !ok {
				t.Fatalf("failed to delete value by key %d, tree size = %d, order = %d", k, tree.Size(), order)
			}

			actualValue := binary.LittleEndian.Uint32(v)
			if expectedValue != actualValue {
				t.Fatalf("expected to delete value %d by key %d, and got %d", expectedValue, k, actualValue)
			}
		}
	}
}

func TestDeleteFromEmptyTree(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")
	tree, err := Open(dbPath, Order(3))
	if err != nil {
		t.Fatalf("failed to open tree: %s", err)
	}

	value, deleted, _ := tree.Delete([]byte{1})
	if deleted {
		t.Fatalf("key %d is deleted, but should not, order %d", 1, 3)
	}
	if value != nil {
		t.Fatalf("value for key %d is not nil: %v", 1, value)
	}
}

func TestDeleteNonExistentElement(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")
	tree, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open tree: %s", err)
	}

	tree.Put([]byte{1}, []byte{2})
	tree.Put([]byte{2}, []byte{2})
	tree.Put([]byte{3}, []byte{3})

	value, deleted, _ := tree.Delete([]byte{4})
	if deleted {
		t.Fatalf("key %d is deleted, but should not, order %d", 4, 3)
	}
	if value != nil {
		t.Fatalf("value for key %d is not nil: %v", 4, value)
	}
}

func TestSize(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")

	expected := 0
	for _, c := range treeCases {
		tree, err := Open(dbPath, Order(3))
		if err != nil {
			t.Fatalf("failed to open tree: %s", err)
		}

		if expected != tree.Size() {
			t.Fatalf("actual size %d is not equal to expected size %d", tree.Size(), expected)
		}

		tree.Put([]byte{c.key}, []byte(c.value))
		expected++

		tree.Close()
	}

	tree, err := Open(dbPath, Order(3))
	if err != nil {
		t.Fatalf("failed to open tree: %s", err)
	}

	if expected != tree.Size() {
		t.Fatalf("actual size %d is not equal to expected size %d", tree.Size(), expected)
	}
}

func TestDeleteMergingThreeTimes(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")
	tree, err := Open(dbPath, Order(3))
	if err != nil {
		t.Fatalf("failed to open tree: %s", err)
	}

	keys := []byte{7, 8, 4, 3, 2, 6, 11, 9, 10, 1, 12, 0, 5}
	for _, v := range keys {
		tree.Put([]byte{v}, []byte{v})
	}

	for _, k := range keys {
		value, deleted, _ := tree.Delete([]byte{k})
		if !deleted {
			t.Fatalf("key %d is not deleted, order %d", k, 3)
		}
		if value == nil {
			t.Fatalf("value for key %d is nil: %v", k, value)
		}
	}
}

func TestDelete(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	for order := 3; order <= 7; order++ {
		dbPath := path.Join(dbDir, fmt.Sprintf("sample_%d.data", order))
		tree, err := Open(dbPath, Order(order))
		if err != nil {
			t.Fatalf("failed to open tree: %s", err)
		}

		for _, c := range treeCases {
			tree.Put([]byte{c.key}, []byte(c.value))
		}

		tree.Close()

		tree, _ = Open(dbPath, Order(order))
		if err != nil {
			t.Fatalf("failed to open tree: %s", err)
		}

		expectedSize := len(treeCases)
		for _, c := range treeCases {
			value, deleted, err := tree.Delete([]byte{c.key})
			expectedSize--

			if err != nil {
				t.Fatalf("failed to delete key %d: %s", c.key, err)
			}
			if !deleted {
				t.Fatalf("key %d is not deleted, order %d", c.key, order)
			}
			if value == nil {
				t.Fatalf("value for key %d is nil: %v", c.key, value)
			}
			if expectedSize != tree.Size() {
				t.Fatalf("the expected size != actual: %d != %d", expectedSize, tree.Size())
			}
		}
	}
}

func TestForEachAfterDeletion(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	dbPath := path.Join(dbDir, "sample.data")
	tree, err := Open(dbPath, Order(3))
	if err != nil {
		t.Fatalf("failed to open tree: %s", err)
	}

	keys := []byte{7, 8, 4, 3, 2, 6, 11, 9, 10, 1, 12, 0, 5}
	for _, v := range keys {
		tree.Put([]byte{v}, []byte{v})
	}

	for i, k := range keys {
		value, deleted, _ := tree.Delete([]byte{k})
		if !deleted {
			t.Fatalf("key %d is not deleted, order %d", k, 3)
		}
		if value == nil {
			t.Fatalf("value for key %d is nil: %v", k, value)
		}

		actual := make([]byte, 0)
		tree.ForEach(func(key []byte, value []byte) {
			actual = append(actual, key...)
		})

		isSorted := sort.SliceIsSorted(actual, func(i, j int) bool {
			return actual[i] < actual[j]
		})
		if !isSorted {
			t.Fatalf("each does not traverse in sorted order, produced result: %s", actual)
		}

		expected := make([]byte, 0)
		for j, k := range keys {
			if j > i {
				expected = append(expected, k)
			}
		}
		sort.Slice(expected, func(i, j int) bool {
			return expected[i] < expected[j]
		})

		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("%v != %v for key %d (%d)", expected, actual, k, i)
		}
	}
}