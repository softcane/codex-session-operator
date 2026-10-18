package job

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Handler exposes the job store through POST /jobs.
func Handler(store *Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			RequestID string `json:"request_id"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			http.Error(w, "invalid submission", http.StatusBadRequest)
			return
		}
		created, err := store.Create(input.RequestID)
		if errors.Is(err, ErrEmptyRequestID) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err != nil {
			http.Error(w, "submission unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(created)
	})
	return mux
}
