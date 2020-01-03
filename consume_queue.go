package qq

import (
	"context"
	"path"

	"encoding/binary"

	"github.com/zhiqiangxu/util/closer"
	"github.com/zhiqiangxu/util/diskqueue"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type consumeQueue interface {
	Put(offset int64) (int64, error)
	Sub(ctx context.Context, status ConsumeStatus) (<-chan diskqueue.StreamBytes, error)
	Close()
}

var _ consumeQueue = (*ConsumeQueue)(nil)

// ConsumeQueue is the smallest unit, each entry is of fixed size
type ConsumeQueue struct {
	broker *Broker
	name   string
	dq     *diskqueue.Queue
	closer *closer.Strict
}

// NewConsumeQueue is ctor for ConsumeQueue
func NewConsumeQueue(broker *Broker, name string) (cq *ConsumeQueue) {
	cq = &ConsumeQueue{broker: broker, name: name, closer: closer.NewStrict()}
	cq.init()
	return
}

func getDirectoryForCQ(dataDir, name string) string {
	return path.Join(dataDir, consumeQueueSubPath, name)
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
	cq.closer.Add(1)
	defer cq.closer.Done()

	var data [ConsumeQueueDataSize]byte

	binary.BigEndian.PutUint64(data[:], uint64(offset))
	cqOffset, err := cq.dq.Put(data[:])
	if err != nil {
		return
	}

	id = cqOffset / ConsumeQueueDataSize
	return
}

// Sub from specified ConsumeStatus,
// the returned channnel should be shared by all consumers of the same group,
// caller should cancel ctx when done Sub, otherwise inner G will never quit(didn't check closer for better performance)
func (cq *ConsumeQueue) Sub(ctx context.Context, status ConsumeStatus) (ch <-chan diskqueue.StreamBytes, err error) {
	cq.closer.Add(1)

	// start StreamRead with ctx from status.NextOffset
	cqCh, err := cq.dq.StreamRead(ctx, status.NextOffset)
	if err != nil {
		cq.closer.Done()
		logger.Instance().Error("ConsumeQueue.StreamRead", zap.Error(err))
		return
	}

	offsetCh := make(chan int64)
	clCh, err := cq.broker.cl.StreamOffsetRead(offsetCh)
	if err != nil {
		cq.closer.Done()
		logger.Instance().Error("ConsumeQueue.Sub", zap.Error(err))
		return
	}

	chRet := make(chan diskqueue.StreamBytes)
	ch = chRet

	go func() {
		defer func() {
			cq.closer.Done()
			close(offsetCh)
		}()

		var (
			cqStreamBytes diskqueue.StreamBytes
			clStreamBytes diskqueue.StreamBytes
			ok            bool
		)

		readCommitLog := func(cqOffset, clOffset int64) bool {
			select {
			case offsetCh <- clOffset:
			case <-ctx.Done():
				return false
			}
			clStreamBytes, ok = <-clCh // clCh will be closed by commit log
			if !ok {
				return false
			}

			select {
			case chRet <- diskqueue.StreamBytes{Bytes: clStreamBytes.Bytes, Offset: cqOffset}:
			case <-ctx.Done():
				return false
			}

			return true
		}

		if len(status.LeftOver) > 0 {
			// deal with left over consume queue offsets
			{
				leftOverOffsetCh := make(chan int64)
				leftOverCh, err := cq.dq.StreamOffsetRead(leftOverOffsetCh)
				if err != nil {
					logger.Instance().Error("cq.dq.StreamOffsetRead", zap.Error(err))
					return
				}
				for _, cqOffset := range status.LeftOver {
					select {
					case leftOverOffsetCh <- cqOffset:
					case <-ctx.Done():
						return
					}

					select {
					case cqStreamBytes, ok = <-leftOverCh:
						if !ok {
							return
						}

						clOffset := int64(binary.BigEndian.Uint64(cqStreamBytes.Bytes))
						if !readCommitLog(cqStreamBytes.Offset, clOffset) {
							return
						}
					case <-ctx.Done():
						return
					}

				}
				close(leftOverOffsetCh)
			}
			// done deal with left over consume queue offsets
		}

		for {
			cqStreamBytes, ok = <-cqCh
			// no need to watch ctx as StreamRead has already done that, which is signaled by ok == false
			if !ok {
				return
			}

			clOffset := int64(binary.BigEndian.Uint64(cqStreamBytes.Bytes))
			if !readCommitLog(cqStreamBytes.Offset, clOffset) {
				return
			}
		}
	}()

	return
}

// Close the consume queue
func (cq *ConsumeQueue) Close() {
	cq.closer.SignalAndWait()

	cq.dq.Close()
}
