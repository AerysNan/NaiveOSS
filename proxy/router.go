package proxy

import (
	"github.com/gorilla/mux"
)

func NewRouter(proxy *ProxyServer) *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/api/bucket", proxy.createBucket).Methods("POST")
	router.HandleFunc("/api/bucket", proxy.deleteBucket).Methods("DELETE")
	router.HandleFunc("/api/bbject", proxy.putObject).Methods("PUT")
	router.HandleFunc("/api/object", proxy.getObject).Methods("GET")
	router.HandleFunc("/api/object", proxy.deleteObject).Methods("DELETE")
	router.HandleFunc("/api/metadata", proxy.getObjectMeta).Methods("GET")
	return router
}
