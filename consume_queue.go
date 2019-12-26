package qq

import "github.com/zhiqiangxu/util/diskqueue"

type consumeQueue interface {
}

// ConsumeQueue for qq
type ConsumeQueue struct {
	dq *diskqueue.Queue
}
