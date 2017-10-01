package main

import (
	"fmt"
	"math/rand"
	"time"
	"ddb/types/table"
	_ "net/http/pprof"
	"log"
	"net/http"
)

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

type u struct {
	Id    int32  `column:"Id"`
	FName string `column:"FName"`
	LName string `column:"LName"`
}

var tab *table.Table
var err error
var t time.Time
var FNames []string
var LNames []string

func init() {
	FNames = []string{"Вася", "Петя", "Саша", "Никита", "Илья", "Олег", "Семен", "Степан", "Иван"}
	LNames = []string{"Иванов", "Петров", "Сидоров", "Проскурин", "Бочаров", "Ефименко", "Дмитриев", "Павленко", "Ивановский", "Петровский", "Сидоровский"}
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	t = time.Now()
	tab, err = table.OpenTable("Users")
	panicIfError(err)

	t = time.Now()
	i := 0
	for i = 0; i < 10000000; i++ {
		tab.Insert(u{Id: int32(i), FName: FNames[rand.Intn(len(FNames))], LName: LNames[rand.Intn(len(LNames))]}, true)
	}
	fmt.Println("Inserted", i, "rows in table ", time.Now().Sub(t))

	t = time.Now()
	tab.Save()
	fmt.Println("Saved", "rows ", time.Now().Sub(t))
}
