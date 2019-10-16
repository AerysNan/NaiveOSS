package osserror

import "errors"

var (
	ErrCorruptedFile          = errors.New("Metadata file corrupted")
	ErrServerInternal         = errors.New("Internal error occur")
	ErrBucketNotExist         = errors.New("Bucket not exist")
	ErrBucketAlreadyExist     = errors.New("Bucket already exist")
	ErrNoStorageAvailable     = errors.New("No available storage")
	ErrObjectMetadataNotFound = errors.New("Object metadata not found")
)
