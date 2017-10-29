package bptree

type Items []Item

type Item interface {
	IsFull() bool
	IsLeaf() bool

	Find(key Key) *Row
	Insert(row *Row)

	split(branch *Branch)
}