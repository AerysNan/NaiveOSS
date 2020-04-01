package global

import "errors"

// Permission level for authentication
const (
	PermissionNone = iota
	PermissionRead
	PermissionWrite
	PermissionOwner
)

// Role indicates whether a user has superuser priviledge
const (
	RoleUser = iota
	RoleAdmin
)

const (
	MaxTransportSize = 1 << 32
	MaxChunkSize     = 1 << 25
)

var ErrorWrongLeader = errors.New("wrong leader")
