package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Aggressive load test - more concurrent clients, connection pooling
func main() {
	url := "http://localhost:8080/events"
	
	// More aggressive configuration
	numGoroutines := 500    // More concurrent clients
	eventsPerGoroutine := 100 // Events per client
	totalEvents := numGoroutines * eventsPerGoroutine
	
	fmt.Printf("Aggressive Load Test Configuration:\n")
	fmt.Printf("  Concurrent clients: %d\n", numGoroutines)
	fmt.Printf("  Events per client: %d\n", eventsPerGoroutine)
	fmt.Printf("  Total events: %d\n\n", totalEvents)
	
	var success atomic.Uint64
	var failed atomic.Uint64
	var dropped atomic.Uint64
	
	var wg sync.WaitGroup
	
	// Create a shared HTTP client with connection pooling
	// This allows reusing connections across goroutines
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			MaxConnsPerHost:     1000,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	
	startTime := time.Now()
	
	// Launch concurrent clients
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		
		go func(clientID int) {
			defer wg.Done()
			
			// Prepare event once (reuse JSON)
			event := map[string]interface{}{
				"data": map[string]interface{}{
					"client_id": clientID,
					"timestamp": time.Now().Unix(),
					"message":   "load test event",
				},
			}
			jsonData, _ := json.Marshal(event)
			
			// Send events rapidly
			for j := 0; j < eventsPerGoroutine; j++ {
				resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))
				if err != nil {
					failed.Add(1)
					continue
				}
				
				// Drain and close body (important for connection reuse)
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				
				if resp.StatusCode == http.StatusAccepted {
					success.Add(1)
				} else if resp.StatusCode == http.StatusServiceUnavailable {
					dropped.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}(i)
	}
	
	// Wait for completion
	wg.Wait()
	
	duration := time.Since(startTime)
	eventsPerSecond := float64(totalEvents) / duration.Seconds()
	
	fmt.Printf("\nResults:\n")
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf("  Events/sec: %.2f\n", eventsPerSecond)
	fmt.Printf("  Successful: %d\n", success.Load())
	fmt.Printf("  Dropped (503): %d\n", dropped.Load())
	fmt.Printf("  Failed: %d\n", failed.Load())
	
	// Get server metrics
	fmt.Println("\nServer Metrics:")
	resp, err := http.Get("http://localhost:8080/metrics")
	if err == nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		
		var metrics map[string]interface{}
		json.Unmarshal(body, &metrics)
		
		for k, v := range metrics {
			fmt.Printf("  %s: %v\n", k, v)
		}
	}
}
