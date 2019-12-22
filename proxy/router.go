package proxy

import (
	"github.com/gorilla/mux"
)

func NewRouter(proxy *Server) *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/api/bucket", proxy.createBucket).Methods("POST")
	router.HandleFunc("/api/bucket", proxy.deleteBucket).Methods("DELETE")
	router.HandleFunc("/api/task", proxy.createUploadID).Methods("POST")
	router.HandleFunc("/api/task", proxy.confirmUploadID).Methods("DELETE")
	router.HandleFunc("/api/object", proxy.putObject).Methods("PUT")
	router.HandleFunc("/api/object", proxy.getObject).Methods("GET")
	router.HandleFunc("/api/object", proxy.deleteObject).Methods("DELETE")
	router.HandleFunc("/api/metadata", proxy.getObjectMeta).Methods("GET")
	router.HandleFunc("/api/auth", proxy.grantUser).Methods("POST")
	router.HandleFunc("/api/user", proxy.loginUser).Methods("POST")
	router.HandleFunc("/api/user", proxy.createUser).Methods("PUT")
	return router
}
