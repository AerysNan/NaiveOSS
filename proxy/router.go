package proxy

import (
	"github.com/gorilla/mux"
)

func NewRouter(proxy *ProxyServer) *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/api/createBucket", proxy.createBucket).Methods("POST")
	router.HandleFunc("/api/putObject", proxy.putObject).Methods("PUT")
	router.HandleFunc("/api/getObject", proxy.getObject).Methods("GET")
	router.HandleFunc("/api/deleteObject", proxy.deleteObject).Methods("DELETE")
	router.HandleFunc("/api/getObjectMeta", proxy.getObjectMeta).Methods("GET")
	return router
}
