# **fbp**tree

[![Build](https://github.com/krasun/fbptree/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/krasun/fbptree/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/krasun/fbptree/branch/main/graph/badge.svg?token=8NU6LR4FQD)](https://codecov.io/gh/krasun/fbptree)
[![Go Report Card](https://goreportcard.com/badge/github.com/krasun/fbptree)](https://goreportcard.com/report/github.com/krasun/fbptree)
[![GoDoc](https://godoc.org/https://godoc.org/github.com/krasun/fbptree?status.svg)](https://godoc.org/github.com/krasun/fbptree)

`fbptree` is a persistent key-value storage engine based on [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree) with byte-slice keys and values. 

## Installation 

To install, run:

```
go get github.com/krasun/fbptree
```

## Usage

An example of usage: 

```go
package fbptree_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/krasun/fbptree"
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

	tree, err := fbptree.Open(dbPath, fbptree.PageSize(4096), fbptree.Order(500))
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

	tree, err = fbptree.Open(dbPath, fbptree.PageSize(4096), fbptree.Order(500))
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
```

## Tests

Run tests with: 

```
$ go test .
ok  	github.com/krasun/fbptree	0.679s
```

## License 

**fbp**tree is released under [the MIT license](LICENSE).
