package main

import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"fmt"
)

func main() {
	db, err := sql.Open("mysql", "root@/mysql?charset=utf8")
	checkErr(err)

	defer db.Close()

	if rows, err := db.Query("SELECT @@some"); err == nil {
		for rows.Next() {
			var res string

			err = rows.Scan(&res)
			checkErr(err)

			fmt.Println("@@max_allowed_packet EEEEE =", res)
		}
	} else {
		panic(err)
	}

	// query
	rows, err := db.Query("SELECT * FROM user")
	checkErr(err)

	for rows.Next() {
		var fname string
		var lname string
		err = rows.Scan(&fname, &lname)
		checkErr(err)
		fmt.Println("first name:", fname, "last name:", lname)
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}