package main

import (
	"log"
	"net/http"
)

func main() {
	fs := http.FileServer(http.Dir("./swagger-ui-5.18.2/dist"))
	http.Handle("/swaggerui/", http.StripPrefix("/swaggerui/", fs))
	log.Fatal(http.ListenAndServe("localhost:"+"8000", fs))
}
