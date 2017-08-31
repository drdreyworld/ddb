package btree

import (
	"fmt"
	"testing"
)

func checkTreeStruct(tree *BTree, t *testing.T) {
	t.Helper()

	if tree.Root() != nil {
		checkItemStruct(tree.root, t)
	}
}

func checkItemStruct(item *tItem, t *testing.T) {
	t.Helper()

	checkIsTrue(item.tree != nil, t, "Item struct invalid: tree is nil")
	checkIsTrue(item.data != nil, t, "Item struct invalid: data is nil")

	if item.Left() != nil || item.Right() != nil {
		checkNotEqual(item.left, item.right, t, "Item struct invalid: left equal right")
	}

	if item.Left() != nil {
		checkEqual(item.GetKey() > item.left.GetKey(), true, t, "Item left key > item key")

		checkNotEqual(item.left, item.parent, t, "Item struct invalid: left equal parent")

		checkItemStruct(item.Left(), t)
	}

	if item.Right() != nil {
		checkEqual(item.GetKey() < item.right.GetKey(), true, t, "Item right key < item key")

		checkItemStruct(item.Right(), t)

		checkNotEqual(item.right, item.parent, t, "Item struct invalid: right equal parent")
	}
}

func checkIsTrue(i bool, t *testing.T, message string) {
	t.Helper()
	if !i {
		t.Fatal(message)
	}
}

func checkIsFalse(i bool, t *testing.T, message string) {
	t.Helper()
	if i {
		t.Fatal(message)
	}
}

func checkEqual(i1, i2 interface{}, t *testing.T, message string) {
	t.Helper()
	if i1 != i2 {
		fmt.Println(i1, "not equal", i2)
		t.Fatal(message)
	}
}

func checkNotEqual(i1, i2 interface{}, t *testing.T, message string) {
	t.Helper()
	if i1 == i2 {
		t.Fatal(message)
	}
}

func TestBTree_CreateItem(t *testing.T) {
	data := Data{10, nil}
	tree := &BTree{}
	item := tree.CreateItem(data)

	checkEqual(item.tree, tree, t, "Item tree mistmatch")
	checkEqual(item.data.key, data.key, t, "Data key mistmatch")
	checkEqual(item.data.value, data.value, t, "Data value mistmatch")
}

func TestBTree_Count(t *testing.T) {
	keys := []int{1, 7, 4, 8, 3, 9, 1, 0}
	cnta := []int{1, 2, 3, 4, 5, 6, 6, 7}
	cntd := []int{1, 1, 2, 3, 4, 5, 6, 7}

	tree := &BTree{}

	for i := 0; i < len(keys); i++ {
		tree.Add(Data{key: keys[i]})
		checkTreeStruct(tree, t)
		checkEqual(tree.Count(), cnta[i], t, fmt.Sprintf("Count after add key %d mistmatch", keys[i]))
	}

	for i := len(keys); i > 0; i-- {
		tree.Delete(keys[i-1])
		checkTreeStruct(tree, t)
		checkEqual(tree.Count(), cntd[i-1]-1, t, fmt.Sprintf("Count after delete key key %d mistmatch", keys[i-1]))
	}
}

func TestBTree_Find(t *testing.T) {
	vals := []Data{
		{key: 1, value: "Value for key 1"},
		{key: 3, value: "Value for key 3"},
		{key: 2, value: "Value for key 2"},
		{key: 7, value: "Value for key 7"},
		{key: 6, value: "Value for key 6"},
		{key: 0, value: "Value for key 0"},
	}

	tree := &BTree{}

	for i := 0; i < len(vals); i++ {
		tree.Add(vals[i])
		checkTreeStruct(tree, t)
	}

	for i := 0; i < len(vals); i++ {
		item := tree.Find(vals[i].key)

		checkIsTrue(item != nil, t, fmt.Sprintf("Added item not found by key %d", vals[i].key))

		checkEqual(item.GetKey(), vals[i].key, t, "Item key mistmatch")
		checkEqual(item.GetValue(), vals[i].value, t, "Item value mistmatch")
	}
}

func TestBTree_Add(t *testing.T) {
	tree := &BTree{}

	// add first
	tree.Add(Data{key: 10})
	checkEqual(1, tree.Count(), t, "Count mistmatch")
	checkTreeStruct(tree, t)

	// add same
	tree.Add(Data{key: 10, value: "new value"})
	checkTreeStruct(tree, t)

	checkEqual(1, tree.Count(), t, "Count mistmatch")

	item := tree.Find(10)
	checkIsTrue(item != nil, t, "Added item not found")

	checkEqual(item.GetKey(), 10, t, "Item key mistmatch")
	checkEqual(item.GetValue(), "new value", t, "Item value mistmatch")

	// add min
	tree.Add(Data{key: 7})
	checkTreeStruct(tree, t)

	checkEqual(10, tree.root.Max().GetKey(), t, "Max mistmatch")
	checkEqual(7, tree.root.Min().GetKey(), t, "Min mistmatch")

	// add max
	tree.Add(Data{key: 17})
	checkTreeStruct(tree, t)

	checkEqual(17, tree.root.Max().GetKey(), t, "Max mistmatch")
	checkEqual(7, tree.root.Min().GetKey(), t, "Min mistmatch")
}

func compare(i1, i2 interface{}) bool {
	return i1 == i2
}

func TestBTree_Delete(t *testing.T) {
	keys := []int{8, 3, 10, 1, 6, 14, 15}
	tree := &BTree{}

	for i := 0; i < len(keys); i++ {
		tree.Add(Data{key: keys[i]})
	}
	checkTreeStruct(tree, t)

	// удаление узла без детей
	tree.Delete(13)
	checkTreeStruct(tree, t)
	checkIsTrue(tree.Find(13) == nil, t, "Item not deleted correct")

	// удаление узла с правым ребенком
	tree.Delete(14)
	checkTreeStruct(tree, t)
	checkIsTrue(tree.Find(14) == nil, t, "Item not deleted correct")

	tree.Add(Data{key: 11})

	// удаление узла с левым ребенком
	tree.Delete(15)
	checkTreeStruct(tree, t)
	checkIsTrue(tree.Find(15) == nil, t, "Item not deleted correct")

	// удаление узла с обоими детьми
	tree.Delete(6)
	checkTreeStruct(tree, t)
	checkIsTrue(tree.Find(6) == nil, t, "Item not deleted correct")

	// удаление корня
	rootKey := tree.root.GetKey()
	tree.Delete(rootKey)
	checkTreeStruct(tree, t)
	checkIsTrue(tree.Find(rootKey) == nil, t, "Item not deleted correct")
}
