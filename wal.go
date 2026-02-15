package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// WAL (Write-Ahead Log) provides durable event storage
type WAL struct {
	dir            string        // Directory for WAL files
	currentFile    *os.File      // Current segment file
	currentSegment int           // Current segment number
	currentSize    int64         // Current file size
	maxSegmentSize int64         // Max size before rotation (100MB)
	mu             sync.Mutex    // Protects file operations
	
	// Metrics
	bytesWritten   atomic.Uint64
	eventsWritten  atomic.Uint64
	fsyncCount     atomic.Uint64
}

// WAL file format:
// [4 bytes: length][N bytes: data][4 bytes: CRC32]
// This allows:
// - Reading variable-length events
// - Detecting corruption
// - Fast sequential writes

// NewWAL creates a new Write-Ahead Log
func NewWAL(dir string) (*WAL, error) {
	// Create WAL directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}
	
	wal := &WAL{
		dir:            dir,
		maxSegmentSize: 100 * 1024 * 1024, // 100 MB
	}
	
	// Open first segment
	if err := wal.rotateSegment(); err != nil {
		return nil, err
	}
	
	log.Printf("WAL initialized in %s", dir)
	return wal, nil
}

// Write appends an event to the WAL
// Format: [length:4][data:N][checksum:4]
func (w *WAL) Write(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// Check if we need to rotate
	entrySize := int64(4 + len(data) + 4) // length + data + checksum
	if w.currentSize + entrySize > w.maxSegmentSize {
		if err := w.rotateSegment(); err != nil {
			return fmt.Errorf("failed to rotate segment: %w", err)
		}
	}
	
	// Calculate checksum
	checksum := crc32.ChecksumIEEE(data)
	
	// Write length (4 bytes, big endian)
	length := uint32(len(data))
	if err := binary.Write(w.currentFile, binary.BigEndian, length); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}
	
	// Write data
	if _, err := w.currentFile.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	
	// Write checksum (4 bytes, big endian)
	if err := binary.Write(w.currentFile, binary.BigEndian, checksum); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}
	
	// Update metrics
	w.currentSize += entrySize
	w.bytesWritten.Add(uint64(entrySize))
	w.eventsWritten.Add(1)
	
	return nil
}

// WriteBatch writes multiple events efficiently
func (w *WAL) WriteBatch(events [][]byte) error {
	for _, event := range events {
		if err := w.Write(event); err != nil {
			return err
		}
	}
	return nil
}

// Sync forces data to disk (fsync)
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.currentFile == nil {
		return nil
	}
	
	if err := w.currentFile.Sync(); err != nil {
		return fmt.Errorf("fsync failed: %w", err)
	}
	
	w.fsyncCount.Add(1)
	return nil
}

// rotateSegment creates a new WAL segment file
func (w *WAL) rotateSegment() error {
	// Close current file if open
	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			log.Printf("Warning: fsync failed during rotation: %v", err)
		}
		if err := w.currentFile.Close(); err != nil {
			log.Printf("Warning: close failed during rotation: %v", err)
		}
	}
	
	// Increment segment number
	w.currentSegment++
	
	// Create new segment file: wal-000001.log, wal-000002.log, etc.
	filename := filepath.Join(w.dir, fmt.Sprintf("wal-%06d.log", w.currentSegment))
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create segment %s: %w", filename, err)
	}
	
	w.currentFile = file
	w.currentSize = 0
	
	log.Printf("Rotated to new WAL segment: %s", filename)
	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.currentFile != nil {
		// Final sync
		if err := w.currentFile.Sync(); err != nil {
			log.Printf("Warning: final fsync failed: %v", err)
		}
		
		// Close file
		if err := w.currentFile.Close(); err != nil {
			return err
		}
		
		w.currentFile = nil
	}
	
	log.Println("WAL closed")
	return nil
}

// GetStats returns WAL statistics
func (w *WAL) GetStats() (int, uint64, uint64, uint64) {
	w.mu.Lock()
	segment := w.currentSegment
	w.mu.Unlock()
	
	return segment, w.bytesWritten.Load(), w.eventsWritten.Load(), w.fsyncCount.Load()
}

// ReadSegment reads all events from a specific WAL segment
// Used for recovery (Phase 3)
func ReadSegment(filename string) ([][]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	var events [][]byte
	
	for {
		// Read length
		var length uint32
		if err := binary.Read(file, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			return events, fmt.Errorf("failed to read length: %w", err)
		}
		
		// Validate length (sanity check)
		if length > 256 {
			return events, fmt.Errorf("invalid event length: %d (max 256)", length)
		}
		
		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(file, data); err != nil {
			return events, fmt.Errorf("failed to read data: %w", err)
		}
		
		// Read checksum
		var storedChecksum uint32
		if err := binary.Read(file, binary.BigEndian, &storedChecksum); err != nil {
			return events, fmt.Errorf("failed to read checksum: %w", err)
		}
		
		// Verify checksum
		computedChecksum := crc32.ChecksumIEEE(data)
		if computedChecksum != storedChecksum {
			return events, fmt.Errorf("checksum mismatch: expected %d, got %d", storedChecksum, computedChecksum)
		}
		
		// Valid event
		events = append(events, data)
	}
	
	return events, nil
}
