package main

import (
	"ddb/types/index/ibtree"
	"fmt"
	"time"
	"log"
	_ "net/http/pprof"
	"net/http"
	"ddb/types/funcs"
	"os"
)

func main() {

	var i int32 = 9999999
	b := funcs.Int32ToBytes(i)

	fmt.Println(i, b)

	i = funcs.Int32FromBytes(b)
	fmt.Println(i, b)

	os.Exit(1)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()


	tree := &ibtree.BTree{
		Degree: 20,
	}
	t := time.Now()
	n := 100
	//n := len(FNames)
	for i := 1; i <= n; i++ {
	//for i := 1; i <= len(FNames); i++ {
	//	name := FNames[rand.Intn(len(FNames))]
	//	name := FNames[i-1]
		name := i
		value := &ibtree.Value{
			Key: ibtree.Key(name),
			Data: name,
		}

		tree.Insert(value)
		//tree.Root.DebugTree()
		//fmt.Println("-----------------------------")
	}
	fmt.Println("insert finished:", time.Now().Sub(t))

	tree.Root.InfixTraverse(func(v *ibtree.Value) bool {
		fmt.Println(v.Data)
		return true
	})
	fmt.Println("-----------------------------")

	//tree.Root.DebugTree()



	fmt.Println("search started")
	t = time.Now()
	for i := 1; i <= n; i++ {
		//name := FNames[rand.Intn(len(FNames))]
		//name := FNames[i]
		name := i
		key := ibtree.Key(name)

		if value := tree.Find(&key); value == nil {
			fmt.Println("not found key:", name)
		} else {
			if name != int(value.Key) || name != value.Data.(int) {
				panic("AAAAAA!")
			}
			//fmt.Println("founded: ", name, ":", string(value.Key), "=", value.Data)
		}
	}
	fmt.Println("search finished:", time.Now().Sub(t))
	//
	//fmt.Println(b)

	tree.Root.DebugTree()
}
