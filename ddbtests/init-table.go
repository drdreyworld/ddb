package ddbtests

import (
	"ddb/driver"
	"math/rand"
)

type u struct {
	Id    int64  `column:"Id"`
	FName string `column:"FName"`
	LName string `column:"LName"`
}

func InitTable() {
	FNames := []string{"Вася", "Петя", "Саша", "Никита", "Илья", "Олег", "Семен", "Степан", "Иван"}
	LNames := []string{"Иванов", "Петров", "Сидоров", "Проскурин", "Бочаров", "Ефименко", "Дмитриев", "Павленко", "Ивановский", "Петровский", "Сидоровский"}
	Columns := []driver.Column{
		driver.Column{Name: "FName", Title: "First name", Length: 50, Type: "string"},
		driver.Column{Name: "LName", Title: "Last name", Length: 50, Type: "string"},
		driver.Column{Name: "Id", Title: "User ID", Length: 64 / 8, Type: "int64"},
	}


	table, err := driver.CreateTable("Users", Columns)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 1000000; i++ {
		table.Insert(u{Id: int64(i), FName: FNames[rand.Intn(len(FNames))], LName: LNames[rand.Intn(len(LNames))]})
	}
	table.Save()

}
