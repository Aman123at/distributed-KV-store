package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

var TerminateLoop bool = false

var dbConn *sql.DB

func handleGetCommand(command []string) {
	if len(command) > 2 {
		fmt.Println("Invalid GET command")
	} else {
		var value string
		// making sure that given key is not expired
		row := dbConn.QueryRow("SELECT store_value FROM kvstore WHERE store_key=? and expiry>UNIX_TIMESTAMP(NOW())", command[1])
		scanerr := row.Scan(&value)
		if scanerr != nil {
			fmt.Printf("Key not present, either expired or unable to retrieve value for key : %s \n", command[1])
		} else {
			fmt.Println(value)
		}
	}

}

func handleSetCommand(command []string) {
	if len(command) != 3 {
		fmt.Println("Invalid SET command")
	} else {
		// setting expiry 1 hour from current time
		_, err := dbConn.Exec("REPLACE INTO kvstore (store_key, store_value, expiry) VALUES (?,?,UNIX_TIMESTAMP(NOW())+3600)", command[1], command[2])
		if err != nil {
			log.Fatalf("Unable to insert key : %v", err)
		} else {
			fmt.Println("Key Inserted successfully")
		}
	}

}

func handleDelCommand(command []string) {
	if len(command) != 2 {
		fmt.Println("Invalid DEL command")
	} else {
		// Doing Soft Delete, Setting expiry to -1 also checking if key is already expired
		res, err := dbConn.Exec("UPDATE kvstore SET expiry=-1 WHERE store_key=? and expiry>UNIX_TIMESTAMP(NOW())", command[1])

		if err != nil {
			fmt.Printf("Key is expired or not present, unable to delete key : %s \n", command[1])
		} else {
			rowsAffected, _ := res.RowsAffected()
			if rowsAffected == 0 {
				fmt.Printf("Key is expired or not present, unable to delete key : %s \n", command[1])
			} else {
				fmt.Println("Key Deleted successfully")
			}
		}
	}
}

func init() {
	db, err := sql.Open("mysql", "root:123456@tcp(localhost:3306)/store")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("DATABASE CONNECTED")
	dbConn = db
}

func main() {
	fmt.Println("Welcome to distributed KV store, We support GET, SET, DEL, EXIT commands. Please enter a command")
	for {
		fmt.Print(">> ")

		// Read commands from CLI
		reader := bufio.NewReader(os.Stdin)
		str, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err.Error())
		}

		var args = []string{}

		trimmedStr := strings.TrimSpace(str)

		if len(trimmedStr) > 0 {
			args = strings.Split(trimmedStr, " ")
			if len(args) > 3 {
				fmt.Println("Invalid command")
			}

			// check first argument for DB operations
			switch args[0] {
			case "GET":
				handleGetCommand(args)
			case "SET":
				handleSetCommand(args)
			case "DEL":
				handleDelCommand(args)
			case "EXIT":
				// Break loop on exit
				TerminateLoop = true
			default:
				fmt.Println("Invalid command")
			}

			if TerminateLoop {
				break
			}
		} else {
			fmt.Println("Plese type command")
		}
	}
}
