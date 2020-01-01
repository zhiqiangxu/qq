package qq

import (
	"path"

	"github.com/zhiqiangxu/util/diskqueue"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type commitLog interface {
	Put([]byte) (int64, error)
}

var _ commitLog = (*CommitLog)(nil)

// CommitLog for qq
type CommitLog struct {
	broker *Broker
	dq     *diskqueue.Queue
}

func getDirectoryForCL(dataDir string) string {
	return path.Join(dataDir, commitLogSubPath)
}

// NewCommitLog is ctor for CommitLog
func NewCommitLog(broker *Broker) *CommitLog {
	dir := getDirectoryForCL(broker.conf.DataDir)
	dq, err := diskqueue.New(diskqueue.Conf{Directory: dir})
	if err != nil {
		logger.Instance().Fatal("NewCommitLog", zap.String("dir", dir), zap.Error(err))
	}
	cl := &CommitLog{broker: broker, dq: dq}

	return cl
}

// Put data into diskqueue
func (cl *CommitLog) Put(data []byte) (offset int64, err error) {
	return cl.dq.Put(data)
}
