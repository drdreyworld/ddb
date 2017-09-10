package main

import (
	"ddb/cdriver"
	"fmt"
	"math/rand"
	"time"
)

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

type u struct {
	Id    int64  `column:"Id"`
	FName string `column:"FName"`
	LName string `column:"LName"`
}

var table *cdriver.Table
var err error
var t time.Time
var FNames []string
var LNames []string

func init() {
	FNames = []string{"Вася", "Петя", "Саша", "Никита", "Илья", "Олег", "Семен", "Степан", "Иван"}
	LNames = []string{"Иванов", "Петров", "Сидоров", "Проскурин", "Бочаров", "Ефименко", "Дмитриев", "Павленко", "Ивановский", "Петровский", "Сидоровский"}
}

func CreateTable() {
	t = time.Now()
	table, err = cdriver.OpenTable("Users")
	panicIfError(err)

	fmt.Println("Table opened:", table.Name, time.Now().Sub(t))
	fmt.Println("Rows count:", table.Columns.GetRowsCount())

	t = time.Now()
	for i := 0; i < 1000000; i++ {
		table.Insert(u{Id: int64(i), FName: FNames[rand.Intn(len(FNames))], LName: LNames[rand.Intn(len(LNames))]})
	}
	fmt.Println("Inserted", table.MaxId, "rows in table ", time.Now().Sub(t))

	t = time.Now()
	table.Columns.Save()
	fmt.Println("Saved", table.MaxId, "rows ", time.Now().Sub(t))
}

func OpenTable() {
	t = time.Now()
	table, err = cdriver.OpenTable("Users")
	panicIfError(err)

	fmt.Println("Table opened:", table.Name, time.Now().Sub(t))
	fmt.Println("Rows count:", table.Columns.GetRowsCount())
}

func main() {
	CreateTable()
	//OpenTable()
	//
	//for i := 0; i < 10; i++ {
	//	t = time.Now()
	//	r, err := table.FindByIndex([]cdriver.FindFieldCond{
	//		{Field: "FName", Value: FNames[rand.Intn(len(FNames))]},
	//		{Field: "LName", Value: LNames[rand.Intn(len(LNames))]},
	//	}, 10, 0)
	//	if err != nil {
	//		fmt.Println("Error:", err)
	//	} else {
	//		fmt.Println("Find rows by 2 columns", time.Now().Sub(t), "results count: ", r.GetRowsCount())
	//		row := u{}
	//		for j := 0; j < 20; j++ {
	//			if err := r.FetchRow(&row); err != nil {
	//				fmt.Println("Fetch row Error:", err)
	//				break
	//			} else {
	//				//fmt.Print(".")
	//				fmt.Println("Fetch row:", row)
	//			}
	//		}
	//		fmt.Println()
	//	}
	//}
	//
	//t = time.Now()
	//r, err := table.FindByIndex([]cdriver.FindFieldCond{{Field: "Id", Value: 12}}, 10, 0)
	//if err != nil {
	//	fmt.Println("Error:", err)
	//} else {
	//	fmt.Println("Find rows by column Id", time.Now().Sub(t), "results count: ", r.GetRowsCount())
	//	row := u{}
	//	if err := r.FetchRow(&row); err != nil {
	//		fmt.Println("Fetch row Error:", err)
	//	} else {
	//		fmt.Println("Fetch row:", row)
	//	}
	//
	//}
}
