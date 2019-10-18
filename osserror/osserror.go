package osserror

import "errors"

var (
	ErrCorruptedFile          = errors.New("Metadata file corrupted")
	ErrServerInternal         = errors.New("Internal error occur")
	ErrBucketNotExist         = errors.New("Bucket not exist")
	ErrEmptyParameter         = errors.New("Empty parameter in HTTP request")
	ErrUnauthenticated        = errors.New("Data operation not authenticated")
	ErrMissingParameter       = errors.New("Missing parameter in HTTP request")
	ErrBucketAlreadyExist     = errors.New("Bucket already exist")
	ErrNoStorageAvailable     = errors.New("No available storage")
	ErrDuplicateConnection    = errors.New("Duplicate connection from a single address")
	ErrObjectMetadataNotFound = errors.New("Object metadata not found")
)
