package qq

import "github.com/zhiqiangxu/util/diskqueue"

type commitLog interface {
}

// CommitLog for qq
type CommitLog struct {
	dq *diskqueue.Queue
}
