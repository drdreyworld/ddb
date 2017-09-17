package main

import (
	"ddb/structs/smap"
	"fmt"
)

var LNames []string

func init() {
	LNames = []string{"Иванов", "Петров", "Сидоров", "Проскурин", "Бочаров", "Ефименко", "Дмитриев", "Павленко", "Ивановский", "Петровский", "Сидоровский"}
}

type SKey string

func (key SKey) Equal(nkey smap.Key) bool {
	if s, ok := nkey.(SKey); ok {
		return s == key
	}
	return false
}

func (key SKey) Less(nkey smap.Key) bool {
	if s, ok := nkey.(SKey); ok {
		return s > key
	}
	return false
}

func (key SKey) Greather(nkey smap.Key) bool {
	if s, ok := nkey.(SKey); ok {
		return s < key
	}
	return false
}

func main() {
	m := smap.SMap{}
	for i := 0; i < len(LNames); i++ {
		key := SKey(LNames[i])
		m.Add(key).Data = LNames[i]
	}
	m.OrderAsc(func(i *smap.Item) bool {
		fmt.Println(i.Data)
		return true
	})
	m.OrderDesc(func(i *smap.Item) bool {
		fmt.Println(i.Data)
		return true
	})
}
