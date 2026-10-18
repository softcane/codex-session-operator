package gatewayclient

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalRejectsSymlinksAndPublicFiles(t *testing.T) {
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(directory, _stateField)
	j, err := openJournal(state)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err := os.Symlink(state, filepath.Join(directory, "link")); err != nil {
		t.Fatal(err)
	}
	if linked, err := openJournal(filepath.Join(directory, "link")); err == nil {
		linked.close()
		t.Fatal("symlink state accepted")
	}
	if err := os.WriteFile(filepath.Join(state, "existing.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("existing.json", filepath.Join(state, "linked.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := j.load("linked"); err == nil {
		t.Fatal("symlink entry adopted")
	}
	if err := os.Chmod(filepath.Join(state, "existing.json"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := j.load("existing"); err == nil {
		t.Fatal("public entry adopted")
	}
	if err := os.Chmod(state, 0755); err != nil {
		t.Fatal(err)
	}
	if public, err := openJournal(state); err == nil {
		public.close()
		t.Fatal("public state accepted")
	}
}

func TestJournalTornFileDoesNotReplaceImmutableOperation(t *testing.T) {
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	j, err := openJournal(filepath.Join(directory, _stateField))
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	op := operation{Version: _journalVersion, ID: _testOperation, Key: _testOperation, Body: json.RawMessage(`{"text":"original"}`)}
	if _, err := j.save(op); err != nil {
		t.Fatal(err)
	}
	if err := j.root.WriteFile(".pending-interrupted", []byte(`{"partial"`), 0600); err != nil {
		t.Fatal(err)
	}
	changed := op
	changed.Body = json.RawMessage(`{"text":"changed"}`)
	if _, err := j.save(changed); err == nil {
		t.Fatal("immutable body overwritten")
	}
	ids, err := j.list()
	if err != nil || len(ids) != 1 || ids[0] != _testOperation {
		t.Fatalf("torn temporary file adopted: %v %v", ids, err)
	}
	restored, err := j.load(_testOperation)
	if err != nil || string(restored.Body) != string(op.Body) {
		t.Fatalf("original operation lost: %+v %v", restored, err)
	}
	for _, name := range []string{"same.json"} {
		info, err := j.root.Stat(name)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("private mode: %v %v", info, err)
		}
	}
}
