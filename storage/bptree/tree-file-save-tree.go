package bptree

import (
	"time"
	"fmt"
)

func (tree *Tree) saveLeafsRoutine() {
	// @todo save in stats
	savecount := 0
	for {
		select {
		case leaf := <-tree.savechan:
			leaf.Lock()
			if leaf.IsChanged() {
				tree.SaveLeaf(leaf)
				savecount++
				leaf.changed = false
			}
			leaf.Unlock()
		case <- time.After(time.Second):
			if tree.close {
				fmt.Println("saveLeafsFunction timeout tree.close:", tree.close)
				fmt.Println("saveLeafsFunction savecount:", savecount)
				tree.savechan = nil
				return
			}
		}
	}
}
