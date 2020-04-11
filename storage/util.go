package storage

const (
	Op_State = iota
	Op_Migrate
	Op_Rotate
	Op_DeleteVolume
	Op_Get
	Op_Create
	Op_Put
	Op_Confirm
	Op_CheckBlob
)

type Op struct {
	Type      int
	ClientId  int64
	CommandId int64
	Args      interface{}
}
