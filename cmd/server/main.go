package main

import (
	"log"

	"github.com/adamwoolhether/proglog/internal/server"
)

func main() {
	// Main function simply creates and starts the server
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
