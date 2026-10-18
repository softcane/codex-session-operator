package job

import (
	"errors"
	"testing"
)

func TestCreateDistinctRequests(t *testing.T) {
	store := NewStore()
	first, err := store.Create("first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Create("second")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || store.Count() != 2 {
		t.Fatalf("distinct submissions = %+v, %+v, count %d", first, second, store.Count())
	}
}

func TestCreateRequiresRequestID(t *testing.T) {
	store := NewStore()
	if _, err := store.Create("  "); !errors.Is(err, ErrEmptyRequestID) {
		t.Fatalf("Create(empty) error = %v", err)
	}
	if store.Count() != 0 {
		t.Fatal("invalid submission created a job")
	}
}
