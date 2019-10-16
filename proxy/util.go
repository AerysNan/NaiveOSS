package proxy

import (
	"net/http"
	"oss/osserror"
)

func checkParameter(w http.ResponseWriter, r *http.Request, parameters []string) []string {
	result := make([]string, 0)
	for _, p := range parameters {
		if len(r.URL.Query()[p]) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(osserror.MissingParameter[p]))
			return nil
		}
		value := r.URL.Query()[p][0]
		if len(value) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(osserror.EmptyParameter[p]))
			return nil
		}
		result = append(result, value)
	}
	return result
}

func writeError(w http.ResponseWriter, err error) {

}
