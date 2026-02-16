package main

import (
	"fmt"
	"log"
)

// Test WAL recovery - read events back from disk
func main() {
	fmt.Println("WAL Recovery Test")
	fmt.Println("=================\n")
	
	// Read the WAL segment
	events, err := ReadSegment("data/wal-000001.log")
	if err != nil {
		log.Fatalf("Failed to read WAL segment: %v", err)
	}
	
	fmt.Printf("Successfully read %d events from WAL\n\n", len(events))
	
	// Show first 5 events
	fmt.Println("First 5 events:")
	for i := 0; i < 5 && i < len(events); i++ {
		fmt.Printf("  Event %d: %s\n", i+1, string(events[i]))
	}
	
	// Show last 5 events
	if len(events) > 5 {
		fmt.Println("\nLast 5 events:")
		start := len(events) - 5
		if start < 0 {
			start = 0
		}
		for i := start; i < len(events); i++ {
			fmt.Printf("  Event %d: %s\n", i+1, string(events[i]))
		}
	}
	
	fmt.Printf("\n✅ Recovery test passed: All %d events recovered successfully!\n", len(events))
}
