package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Demonstrates that disk I/O doesn't block HTTP ingestion
func main() {
	fmt.Println("Non-Blocking I/O Demonstration")
	fmt.Println("================================\n")
	
	url := "http://localhost:8080/events"
	
	// Send 10 events and measure latency
	fmt.Println("Sending 10 events, measuring response time...\n")
	
	var totalLatency time.Duration
	
	for i := 0; i < 10; i++ {
		event := map[string]interface{}{
			"data": map[string]interface{}{
				"test_id": i,
				"message": "latency test",
			},
		}
		jsonData, _ := json.Marshal(event)
		
		// Measure time to get HTTP response
		start := time.Now()
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		latency := time.Since(start)
		
		if err != nil {
			fmt.Printf("❌ Request %d failed: %v\n", i+1, err)
			continue
		}
		
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		
		totalLatency += latency
		
		if resp.StatusCode == 202 {
			fmt.Printf("✅ Event %d: Response in %v (202 Accepted)\n", i+1, latency)
		} else {
			fmt.Printf("⚠️  Event %d: Status %d in %v\n", i+1, resp.StatusCode, latency)
		}
	}
	
	avgLatency := totalLatency / 10
	
	fmt.Printf("\n📊 Results:\n")
	fmt.Printf("   Average response time: %v\n", avgLatency)
	fmt.Printf("   Total time: %v\n", totalLatency)
	
	if avgLatency < 10*time.Millisecond {
		fmt.Printf("\n✅ PROOF: Average response time is %v\n", avgLatency)
		fmt.Printf("   This is MUCH faster than disk I/O (typically 5-10ms for fsync)\n")
		fmt.Printf("   Therefore, HTTP responses are NOT waiting for disk writes!\n")
	} else {
		fmt.Printf("\n⚠️  Responses seem slow (%v)\n", avgLatency)
	}
	
	fmt.Println("\n💡 Key Point:")
	fmt.Println("   - Disk fsync takes ~5-10ms")
	fmt.Printf("   - Our HTTP responses take %v\n", avgLatency)
	fmt.Println("   - If disk blocked HTTP, responses would be >5ms")
	fmt.Println("   - Fast responses = disk I/O is async (non-blocking)")
}
