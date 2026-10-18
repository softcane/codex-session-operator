// Package job implements the in-memory job submission service.
package job

import (
	"errors"
	"strings"
	"sync"
)

// ErrEmptyRequestID reports a submission without a caller replay identity.
var ErrEmptyRequestID = errors.New("request ID is required")

// Job is a submitted unit of work.
type Job struct {
	ID        int    `json:"id"`
	RequestID string `json:"request_id"`
}

// Store keeps jobs and their request identities safe for concurrent callers.
type Store struct {
	mu       sync.Mutex
	nextID   int
	jobs     map[int]Job
	requests map[string]int
}

// NewStore creates an empty job store.
func NewStore() *Store {
	return &Store{jobs: make(map[int]Job), requests: make(map[string]int)}
}

// Create submits a job. A caller can retry a request using its original ID.
func (s *Store) Create(requestID string) (Job, error) {
	if strings.TrimSpace(requestID) == "" {
		return Job{}, ErrEmptyRequestID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	created := Job{ID: s.nextID, RequestID: requestID}
	s.jobs[created.ID] = created
	s.requests[requestID] = created.ID
	return created, nil
}

// Count reports the number of stored jobs.
func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.jobs)
}
