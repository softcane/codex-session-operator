package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"example.com/my-go-project/internal/job"
)

func main() {
	address := os.Getenv("LISTEN_ADDRESS")
	if address == "" {
		address = ":8080"
	}
	server := &http.Server{Addr: address, Handler: job.Handler(job.NewStore()), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
