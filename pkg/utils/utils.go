package utils

import (
	"encoding/json"
	"log"
)

func PrettyPrint(values ...interface{}) { // use variadic parameters
	for _, value := range values {
		prettyData, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			log.Printf("Error pretty printing data: %v", err)
			continue
		}
		log.Printf("\n%s\n", prettyData)
	}
}
