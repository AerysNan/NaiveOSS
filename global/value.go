package global

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
