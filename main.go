package main

import (
	"ddb/ddbtests"
	"ddb/driver"
	"log"
	"time"
)

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func main1() {
	ddbtests.InitTable()
}

func main() {
	tm := time.Now()
	t, err := driver.OpenTable("Users")
	panicIfError(err)
	log.Println("table opened: ", time.Now().Sub(tm))

	log.Println()
	res := t.Find("FName", "Вася", 20)
	log.Println("result:", res)

	log.Println()
	res = t.FindByCond([]driver.FindFieldCond{
		{Field: "FName", Value: "Вася"},
		{Field: "LName", Value: "Иванов"},
	}, 20)
	log.Println("result:", res)

	log.Println()
	r := t.CountByCond([]driver.FindFieldCond{
		{Field: "FName", Value: "Вася"},
		{Field: "LName", Value: "Иванов"},
	})
	log.Println("result:", r)

	log.Println()
	res = t.Find("Id", 900000, 1)
	log.Println("result:", res)
}