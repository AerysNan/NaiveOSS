package proxy

import (
	"net/http"
	"oss/osserror"
)

func checkParameter(r *http.Request, parameters []string) ([]string, error) {
	result := make([]string, 0)
	for _, p := range parameters {
		if len(r.URL.Query()[p]) == 0 {
			return nil, osserror.ErrMissingParameter
		}
		value := r.URL.Query()[p][0]
		if len(value) == 0 {
			return nil, osserror.ErrEmptyParameter
		}
		result = append(result, value)
	}
	return result, nil
}

func writeError(w http.ResponseWriter, err error) {
	switch err {
	case osserror.ErrCorruptedFile:
		w.WriteHeader(http.StatusInternalServerError)
	case osserror.ErrServerInternal:
		w.WriteHeader(http.StatusInternalServerError)
	case osserror.ErrBucketNotExist:
		w.WriteHeader(http.StatusNotFound)
	case osserror.ErrEmptyParameter:
		w.WriteHeader(http.StatusBadRequest)
	case osserror.ErrMissingParameter:
		w.WriteHeader(http.StatusBadRequest)
	case osserror.ErrBucketAlreadyExist:
		w.WriteHeader(http.StatusConflict)
	case osserror.ErrNoStorageAvailable:
		w.WriteHeader(http.StatusInsufficientStorage)
	case osserror.ErrObjectMetadataNotFound:
		w.WriteHeader(http.StatusNotFound)
	case osserror.ErrUnauthenticated:
		w.WriteHeader(http.StatusUnauthorized)
	}
	_, _ = w.Write([]byte(err.Error()))
}
