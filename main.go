package main

import (
	"ddb/types/key"
	"fmt"
	"ddb/types/index/btree"
)

var Count int
var Values btree.Values

func Find(k key.BytesKey) *btree.Value {
	if Count == 0 {
		return nil
	}

	minj := 0
	maxj := Count

	i := 0
	l := len(k)

	fmt.Println("key", k, string(k))

	for i = 0; i < l; i++ {
		for maxj > 0 && k[i] < Values[maxj - 1].Key[i] {
			maxj--
		}
		for minj < maxj && k[i] > Values[minj].Key[i] {
			minj++
		}
	}

	fmt.Println("min", minj, "max", maxj)

	if minj == maxj {
		fmt.Println("Нужно искать в Items[", maxj ,"]")
		return nil
		//return item.Items[minj].Find(minj)
	}

	// нашли значение
	if minj < maxj {
		return Values[minj]
	}

	return nil
}

func main() {
	Values = btree.Values{}

	Values = append(Values, &btree.Value{ Key : key.BytesKey([]byte("Валера")) })
	Values = append(Values, &btree.Value{ Key : key.BytesKey([]byte("Вася  ")) })
	Values = append(Values, &btree.Value{ Key : key.BytesKey([]byte("Витя  ")) })
	Values = append(Values, &btree.Value{ Key : key.BytesKey([]byte("Иван  ")) })
	Values = append(Values, &btree.Value{ Key : key.BytesKey([]byte("Степан")) })
	//Values = append(Values, &btree.Value{ Key : key.BytesKey([]byte("Степка")) })

	Count = 5

	if value := Find(key.BytesKey([]byte("Иван  "))); value == nil {
		fmt.Println("Иван не найден")
	} else {
		fmt.Println("Ивано найден: ", string(value.Key))
	}

	fmt.Println()

	if value := Find(key.BytesKey([]byte("Вааня "))); value == nil {
		fmt.Println("Вааня не найден")
	} else {
		fmt.Println("Вааня найден: ", string(value.Key))
	}

	fmt.Println()

	if value := Find(key.BytesKey([]byte("Ватя  "))); value == nil {
		fmt.Println("Ватя не найден")
	} else {
		fmt.Println("Ватя найден: ", string(value.Key))
	}

	fmt.Println()

	if value := Find(key.BytesKey([]byte("Степан"))); value == nil {
		fmt.Println("Степан не найден")
	} else {
		fmt.Println("Степан найден: ", string(value.Key))
	}

	fmt.Println()

}
