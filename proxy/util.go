package proxy

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func checkParameter(r *http.Request, parameters []string) ([]string, error) {
	result := make([]string, 0)
	for _, p := range parameters {
		value := r.Header.Get(p)
		if len(value) == 0 {
			return nil, status.Error(codes.Unknown, "missing parameter")
		}
		result = append(result, value)
	}
	return result, nil
}

func writeError(w http.ResponseWriter, err error) {
	if err == nil {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
		return
	}
	s, ok := status.FromError(err)
	if !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	switch s.Code() {
	case codes.OK:
		w.WriteHeader(http.StatusForbidden)
	case codes.Canceled:
		w.WriteHeader(http.StatusNotAcceptable)
	case codes.Unknown:
		w.WriteHeader(http.StatusForbidden)
	case codes.InvalidArgument:
		w.WriteHeader(http.StatusBadRequest)
	case codes.DeadlineExceeded:
		w.WriteHeader(http.StatusRequestTimeout)
	case codes.NotFound:
		w.WriteHeader(http.StatusNotFound)
	case codes.AlreadyExists:
		w.WriteHeader(http.StatusConflict)
	case codes.PermissionDenied:
		w.WriteHeader(http.StatusUnauthorized)
	case codes.ResourceExhausted:
		w.WriteHeader(http.StatusInsufficientStorage)
	case codes.FailedPrecondition:
		w.WriteHeader(http.StatusFailedDependency)
	case codes.Aborted:
		w.WriteHeader(http.StatusForbidden)
	case codes.OutOfRange:
		w.WriteHeader(http.StatusBadRequest)
	case codes.Unimplemented:
		w.WriteHeader(http.StatusNotImplemented)
	case codes.Internal:
		w.WriteHeader(http.StatusInternalServerError)
	case codes.Unavailable:
		w.WriteHeader(http.StatusUnavailableForLegalReasons)
	case codes.DataLoss:
		w.WriteHeader(http.StatusUnauthorized)
	case codes.Unauthenticated:
		w.WriteHeader(http.StatusUnauthorized)
	}
	_, _ = w.Write([]byte(s.Message()))
}

func writeResponse(w http.ResponseWriter, body []byte) {
	w.WriteHeader(http.StatusOK)
	if body == nil {
		_, _ = w.Write([]byte("OK"))
		return
	}
	_, _ = w.Write(body)
}
