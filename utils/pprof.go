package utils

import (
	"net/http"
	"net/http/pprof"
)

func StartHttpDebugger() {
	// http://localhost:7890/debug/pprof/
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: ":7890", Handler: pprofHandler}
	go server.ListenAndServe()
}
