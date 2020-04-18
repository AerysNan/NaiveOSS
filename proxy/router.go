package proxy

import (
	"net/http/pprof"
	_ "net/http/pprof"

	"github.com/gorilla/mux"
)

func NewRouter(proxy *Server) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/api/bucket", proxy.listBucket).Methods("GET")
	router.HandleFunc("/api/bucket", proxy.createBucket).Methods("POST")
	router.HandleFunc("/api/bucket", proxy.deleteBucket).Methods("DELETE")
	router.HandleFunc("/api/task", proxy.createUploadID).Methods("POST")
	router.HandleFunc("/api/task", proxy.confirmUploadID).Methods("DELETE")
	router.HandleFunc("/api/object", proxy.putObject).Methods("PUT")
	router.HandleFunc("/api/object", proxy.getObject).Methods("GET")
	router.HandleFunc("/api/object", proxy.deleteObject).Methods("DELETE")
	router.HandleFunc("/api/object", proxy.listObject).Methods("POST")
	router.HandleFunc("/api/metadata", proxy.getObjectMeta).Methods("GET")
	router.HandleFunc("/api/metadata", proxy.rangeObject).Methods("POST")
	router.HandleFunc("/api/auth", proxy.grantUser).Methods("POST")
	router.HandleFunc("/api/user", proxy.loginUser).Methods("POST")
	router.HandleFunc("/api/user", proxy.createUser).Methods("PUT")

	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)
	router.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	router.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	router.Handle("/debug/pprof/block", pprof.Handler("block"))
	return router
}
