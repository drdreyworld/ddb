package bptree

import (
	"errors"
	"fmt"
	"os"
	"time"
)

const ERR_FILE_ALREADY_OPENED = "File is already opened!"

func (tree *Tree) OpenFile(filename string) (err error) {
	if tree.file != nil {
		return errors.New(ERR_FILE_ALREADY_OPENED)
	}

	tree.file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return err
	}

	// @todo save in stats
	t := time.Now()
	err = tree.LoadTree()
	if err != nil {
		return err
	}
	fmt.Println("time to load tree structure:", time.Now().Sub(t))

	tree.savechan = make(chan *Leaf, 1000000)

	go tree.saveLeafsRoutine()

	return nil
}

func (tree *Tree) CloseFile() error {
	tree.Lock()
	tree.close = true
	tree.Unlock()
	if tree.file != nil {
		for tree.savechan != nil {
			fmt.Println("wait for all leaf saved")
			time.Sleep(1 * time.Second)
		}

		if err := tree.file.Sync(); err != nil {
			return err
		}
		return tree.file.Close()
	}
	return nil
}
