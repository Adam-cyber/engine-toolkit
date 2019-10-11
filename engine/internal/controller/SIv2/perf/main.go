package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type taskInfo struct {
	IngestionTime string `json:"ingestionTime,omitempty"`
}

func main() {
	var totalDur time.Duration
	var numLines int

	scanner := bufio.NewScanner(os.Stdin)
	for {
		if !scanner.Scan() {
			break
		}

		text := scanner.Text()
		if text == "" {
			continue
		}

		var info taskInfo
		err := json.Unmarshal([]byte(text), &info)
		if err != nil {
			log.Fatalf("invalid input: %s", err)
		}

		fmt.Println(info.IngestionTime)

		dur, err := time.ParseDuration(info.IngestionTime)
		if err != nil {
			log.Fatalf("invalid duration %q: %s", info.IngestionTime, err)
		}

		totalDur += dur
		numLines++
	}

	if numLines > 0 {
		avgDur := totalDur / time.Duration(numLines)
		fmt.Printf("average duration: %s\n", avgDur.String())
	}
}
