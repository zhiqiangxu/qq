package qq

import (
	"context"
	"path"

	"encoding/binary"

	"github.com/zhiqiangxu/util/diskqueue"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type consumeQueue interface {
	Put(offset int64) (int64, error)
}

var _ consumeQueue = (*ConsumeQueue)(nil)

// ConsumeQueue is the smallest unit, each entry is of fixed size
type ConsumeQueue struct {
	broker *Broker
	name   string
	dq     *diskqueue.Queue
}

// NewConsumeQueue is ctor for ConsumeQueue
func NewConsumeQueue(broker *Broker, name string) (cq *ConsumeQueue) {
	cq = &ConsumeQueue{broker: broker, name: name}
	cq.init()
	return
}

func getDirectoryForCQ(dataDir, name string) string {
	return path.Join(dataDir, name)
}

// ConsumeQueueCustomDecoder for consume queue
func ConsumeQueueCustomDecoder(ctx context.Context, r *diskqueue.QfileSizeReader) (anotherFile bool, data []byte, err error) {
	data = make([]byte, ConsumeQueueDataSize)
	err = r.Read(ctx, data)

	return
}

func (cq *ConsumeQueue) init() {
	dir := getDirectoryForCQ(cq.broker.conf.DataDir, cq.name)
	dq, err := diskqueue.New(diskqueue.Conf{Directory: dir, CustomDecoder: ConsumeQueueCustomDecoder})
	if err != nil {
		logger.Instance().Fatal("ConsumeQueue.init", zap.String("dir", dir), zap.Error(err))
	}

	cq.dq = dq
	return
}

// Put the commit log offset into consume queue
func (cq *ConsumeQueue) Put(offset int64) (id int64, err error) {
	var data [ConsumeQueueDataSize]byte

	binary.BigEndian.PutUint64(data[:], uint64(offset))
	cqOffset, err := cq.dq.Put(data[:])
	if err != nil {
		return
	}

	id = cqOffset / ConsumeQueueDataSize
	return
}
