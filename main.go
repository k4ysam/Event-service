package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Event represents an incoming event (max 256 bytes)
type Event struct {
	Data json.RawMessage `json:"data"` // RawMessage preserves the original JSON
}

// RingBuffer is a fixed-size circular buffer for events
// This ensures bounded memory usage
type RingBuffer struct {
	buffer   [][]byte      // Fixed-size array of byte slices
	size     int           // Total capacity
	writeIdx int           // Where to write next
	readIdx  int           // Where to read next
	count    int           // Current number of items
	mu       sync.Mutex    // Mutex for thread safety (like Java's synchronized)
	notEmpty *sync.Cond    // Condition variable for signaling
	
	// Metrics (atomic for lock-free reading)
	totalReceived atomic.Uint64
	totalDropped  atomic.Uint64
}

// NewRingBuffer creates a new ring buffer with the specified size
func NewRingBuffer(size int) *RingBuffer {
	rb := &RingBuffer{
		buffer: make([][]byte, size), // Similar to: new byte[size][] in Java
		size:   size,
	}
	rb.notEmpty = sync.NewCond(&rb.mu)
	return rb
}

// Push adds an event to the buffer
// Returns false if buffer is full (event dropped)
func (rb *RingBuffer) Push(event []byte) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock() // Like try-finally in Java, ensures unlock
	
	rb.totalReceived.Add(1)
	
	// Buffer is full - drop the event
	if rb.count == rb.size {
		rb.totalDropped.Add(1)
		return false
	}
	
	// Make a copy of the event data (important for safety)
	eventCopy := make([]byte, len(event))
	copy(eventCopy, event)
	
	rb.buffer[rb.writeIdx] = eventCopy
	rb.writeIdx = (rb.writeIdx + 1) % rb.size // Circular: wrap around
	rb.count++
	
	// Signal that buffer is not empty (wake up consumer)
	rb.notEmpty.Signal()
	
	return true
}

// Pop removes and returns an event from the buffer
// Blocks if buffer is empty (waits for data)
func (rb *RingBuffer) Pop() []byte {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	
	// Wait while buffer is empty
	for rb.count == 0 {
		rb.notEmpty.Wait() // Releases lock and waits (like Java's wait())
	}
	
	event := rb.buffer[rb.readIdx]
	rb.buffer[rb.readIdx] = nil // Help GC
	rb.readIdx = (rb.readIdx + 1) % rb.size
	rb.count--
	
	return event
}

// GetStats returns current buffer statistics
func (rb *RingBuffer) GetStats() (int, uint64, uint64) {
	rb.mu.Lock()
	count := rb.count
	rb.mu.Unlock()
	
	return count, rb.totalReceived.Load(), rb.totalDropped.Load()
}

// Server holds our application state
type Server struct {
	buffer *RingBuffer
	wal    *WAL
}

// handleEvents processes POST requests to /events
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	// Read the request body (max 256 bytes + some buffer)
	body, err := io.ReadAll(io.LimitReader(r.Body, 512))
	if err != nil {
		http.Error(w, "Failed to read request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	
	// Validate size constraint (max 256 bytes)
	if len(body) > 256 {
		http.Error(w, "Event too large (max 256 bytes)", http.StatusRequestEntityTooLarge)
		return
	}
	
	// Validate it's proper JSON
	var event Event
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	
	// Try to push to buffer
	if !s.buffer.Push(body) {
		// Buffer full - return 503 Service Unavailable
		http.Error(w, "Buffer full, try again later", http.StatusServiceUnavailable)
		return
	}
	
	// Success! Return 202 Accepted (async processing)
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"accepted"}`))
}

// handleHealth provides a simple health check endpoint
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy"}`))
}

// handleMetrics provides buffer and WAL statistics
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	count, received, dropped := s.buffer.GetStats()
	utilization := float64(count) / float64(s.buffer.size) * 100
	
	segment, bytesWritten, eventsWritten, fsyncCount := s.wal.GetStats()
	
	// Return metrics as JSON
	metrics := map[string]interface{}{
		"buffer_current":      count,
		"buffer_capacity":     s.buffer.size,
		"buffer_utilization":  fmt.Sprintf("%.2f%%", utilization),
		"events_received":     received,
		"events_dropped":      dropped,
		"wal_current_segment": segment,
		"wal_bytes_written":   bytesWritten,
		"wal_events_written":  eventsWritten,
		"wal_fsync_count":     fsyncCount,
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// diskWriter writes events from buffer to WAL
func (s *Server) diskWriter() {
	log.Println("Disk writer started - writing to WAL")
	
	batch := make([][]byte, 0, 1000)
	lastSync := time.Now()
	syncInterval := 100 * time.Millisecond // Sync every 100ms
	
	for {
		// Pop an event from the buffer (blocks if empty)
		event := s.buffer.Pop()
		batch = append(batch, event)
		
		// Write batch when full or after timeout
		shouldWrite := len(batch) >= 1000 || time.Since(lastSync) > syncInterval
		
		if shouldWrite {
			// Write batch to WAL
			if err := s.wal.WriteBatch(batch); err != nil {
				log.Printf("ERROR: Failed to write batch to WAL: %v", err)
				// In production, would need proper error handling
				// For now, continue and try next batch
			}
			
			// Fsync to disk (durable write)
			if err := s.wal.Sync(); err != nil {
				log.Printf("ERROR: Failed to fsync WAL: %v", err)
			}
			
			// Clear batch
			batch = batch[:0]
			lastSync = time.Now()
		}
	}
}

func main() {
	// Create ring buffer with 100k capacity
	// Memory usage: ~100k * 256 bytes = ~25 MB
	bufferSize := 100000
	buffer := NewRingBuffer(bufferSize)
	
	// Create WAL (Write-Ahead Log) in ./data directory
	wal, err := NewWAL("./data")
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()
	
	server := &Server{
		buffer: buffer,
		wal:    wal,
	}
	
	// Start background disk writer goroutine (like a Java thread)
	go server.diskWriter()
	
	// Register HTTP handlers (like Spring @RequestMapping)
	http.HandleFunc("/events", server.handleEvents)
	http.HandleFunc("/health", server.handleHealth)
	http.HandleFunc("/metrics", server.handleMetrics)
	
	// Start HTTP server
	addr := ":8080"
	log.Printf("Server starting on %s", addr)
	log.Printf("Buffer capacity: %d events (~%.2f MB max)", bufferSize, float64(bufferSize*256)/(1024*1024))
	log.Printf("WAL directory: ./data")
	log.Println("Endpoints:")
	log.Println("  POST /events  - Submit events")
	log.Println("  GET  /health  - Health check")
	log.Println("  GET  /metrics - Buffer + WAL metrics")
	
	// This blocks and runs the server (like Spring Boot's main method)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
