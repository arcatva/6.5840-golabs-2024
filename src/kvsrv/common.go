package kvsrv

type Args interface {
	getClerkId() int64
	getOpId() int64
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	OpId   int64
}

func (a *PutAppendArgs) getClerkId() int64 {
	return a.ClerkId
}

func (a *PutAppendArgs) getOpId() int64 {
	return a.OpId
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	OpId   int64
}

func (a *GetArgs) getClerkId() int64 {
	return a.ClerkId
}
func (a *GetArgs) getOpId() int64 {
	return a.OpId
}

type GetReply struct {
	Value string
}
