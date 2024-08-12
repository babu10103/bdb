package main

import (
	"encoding/json"
	"fmt"
	"github.com/babu10103/bdb/bdb"
	"github.com/jcelliott/lumber"
)

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func main() {
	employees := []User{
		{"John", "23", "23344333", "Myrl Tech", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Paul", "25", "23344333", "Google", Address{"san francisco", "california", "USA", "410013"}},
		{"Robert", "27", "23344333", "Microsoft", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Vince", "29", "23344333", "Facebook", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Neo", "31", "23344333", "Remote-Teams", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Albert", "32", "23344333", "Dominate", Address{"bangalore", "karnataka", "india", "410013"}},
	}

	opts := bdb.Options{
		Logger: lumber.NewConsoleLogger((lumber.DEBUG)),
	}
	db, err := bdb.New("mydb", &opts)

	if err != nil {
		fmt.Println(err)
		return
	}

	for _, user := range employees {
		db.Write("employees", user)
	}

	var user User
	err = db.Read("employees", "apz01zicapy9eqixuc9uehq235", &user)
	if err != nil {
		fmt.Println("Error", err)
	}
	db.Update("employees", "apz01zicapy9eqixuc9uehq235", User{
		Address: Address{Pincode: "515671"},
	})

	err = db.Read("employees", "apz01zicapy9eqixuc9uehq235", &user)
	if err != nil {
		fmt.Println("Error", err)
	}
	fmt.Printf("After update:  %+v\n", user)

}
