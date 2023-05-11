package utils

import (
	"encoding/json"
	"fmt"
	"log"
)

func PrettyPrint(data interface{}) {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Println("Error marshaling data:", err)
		return
	}
	fmt.Println(string(jsonData))
}
