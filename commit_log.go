package qq

import "github.com/zhiqiangxu/util/diskqueue"

type commitLog interface {
	Put([]byte) (int64, error)
}

var _ commitLog = (*CommitLog)(nil)

// CommitLog for qq
type CommitLog struct {
	dq *diskqueue.Queue
}

// Put data into diskqueue
func (cl *CommitLog) Put(data []byte) (offset int64, err error) {
	return cl.dq.Put(data)
}
