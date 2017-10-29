package main

import (
	"ddb/storage/bptree"
	"fmt"
	"time"
	"math/rand"
	"log"
	"net/http"
	_ "net/http/pprof"
)

var FNames []string
var LNames []string
var t time.Time

func init() {
	FNames = []string{"Вася", "Петя", "Саша", "Никита", "Илья", "Олег", "Семен", "Степан", "Иван"}
	LNames = []string{"Иванов", "Петров", "Сидоров", "Проскурин", "Бочаров", "Ефименко", "Дмитриев", "Павленко", "Ивановский", "Петровский", "Сидоровский"}
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	index := bptree.Tree{}
	index.Init(100, 100)
	index.Mid = 20

	err := index.OpenFile("/Users/andrey/Go/src/ddb/storage/bin/index.idx")
	if err != nil {
		panic(err)
	}
	defer index.CloseFile()

	keys := []bptree.Key{}

	if true {
		count := 1000000
		t = time.Now()
		for i := 0; i < count; i++ {

			value := fmt.Sprintf("%s %s %d %d %d",
				FNames[rand.Intn(len(FNames))],
				LNames[rand.Intn(len(LNames))],
				rand.Intn(10000000),
				rand.Intn(10000000),
				rand.Intn(10000000),
			)

			keys = append(keys, []byte(value))

			index.Insert(bptree.CreateRow(100, 100, []byte(value), []byte(value)))
		}
		fmt.Println("Inserted", count, "rows in table ", time.Now().Sub(t))
	}

	fmt.Println("Tree height:", index.GetHeight())

	//index.Root.DebugTree(0)
	//
	//t = time.Now()
	//index.ScanRows(func(row *bptree.Row) {
	//	//fmt.Println(string(row.Key()))
	//	keys = append(keys, row.Key())
	//}, bptree.SCAN_DIRECTION_ASC)
	//fmt.Println("Time to load and set keys:", time.Now().Sub(t))
	//fmt.Println()
	//

	fmt.Println()
	fmt.Println()


	rowscount := 0
	leafscount := 0

	t = time.Now()
	index.ScanLeafs(func(leaf *bptree.Leaf) {
		if leaf.Count() == 0 {
			panic("leaf with count = 0")
		}
		rowscount += leaf.Count()
		leafscount++
	}, bptree.SCAN_DIRECTION_ASC)
	fmt.Println("Time to scan leafs:", time.Now().Sub(t))
	fmt.Println("total rowscount:", rowscount)
	fmt.Println("total leafscount:", leafscount)

	fmt.Println()
	fmt.Println()

	//pagesloaded := 0
	//t = time.Now()
	//index.ScanLeafs(func(leaf *bptree.Leaf) {
	//	if rand.Intn(1000) > 998 {
	//		pagesloaded++
	//		//fmt.Println("")
	//		//fmt.Println("scan leaf page")
	//		leaf.ScanRowsASC(func(row *bptree.Row) {
	//			//fmt.Println("   ", string(row.Key()))
	//			keys = append(keys, row.Key())
	//		})
	//		leaf.Unload()
	//		//fmt.Println("")
	//		//fmt.Println("")
	//	}
	//	// check saved
	//	//leaf.Unload()
	//}, bptree.SCAN_DIRECTION_ASC)
	//fmt.Println("Time to rand read scan leafs:", time.Now().Sub(t))
	//fmt.Println("Pages loaded:", pagesloaded)

	fmt.Println("Find")

	t = time.Now()
	for i := 0; i < len(keys); i++ {
		row := index.Find(keys[i])
		if row != nil {
			//fmt.Println("found:", string(row.Key()))
		} else {
			panic("not found " + string(keys[i]))
		}
	}
	fmt.Println("Time to find", len(keys), "keys:", time.Now().Sub(t))
}
