package qq

import (
	"sync"

	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type metaManager interface {
	CreateExchange(name string, exType int32) error
	DeleteExchange(name string) error
	GetExchange(name string) *Exchange
	GetOrCreateConsumeQueue(name string) *ConsumeQueue
	ScanTopics(cursor string, limit int) ([]string, error)
}

var _ metaManager = (*MetaManager)(nil)

// MetaManager for qq
type MetaManager struct {
	exRWL  sync.RWMutex
	allEx  map[string]*Exchange
	store  *Store
	broker *Broker
}

// NewMetaManager is ctor for MetaManager
func NewMetaManager(broker *Broker) (mm *MetaManager) {
	mm = &MetaManager{allEx: make(map[string]*Exchange), broker: broker}
	mm.allEx[defaultExchangeName] = NewExchange(ExchangeTypeDirect)
	mm.store = OpenOrCreateStore(broker.conf.DataDir)
	err := mm.init()
	if err != nil {
		logger.Instance().Fatal("MetaManager.init", zap.Error(err))
	}
	return
}

func (mm *MetaManager) init() error {
	return nil
}

// CreateExchange will create an exchange if not exists
func (mm *MetaManager) CreateExchange(name string, exType int32) error {
	return nil
}

// DeleteExchange will delete an exchange if exists
func (mm *MetaManager) DeleteExchange(name string) error {
	return nil
}

// GetExchange by name
func (mm *MetaManager) GetExchange(name string) *Exchange {
	return nil
}

// GetOrCreateConsumeQueue will create a ConsumeQueue if not exists
func (mm *MetaManager) GetOrCreateConsumeQueue(name string) *ConsumeQueue {
	return nil
}

// ScanTopics for iterate over topics
func (mm *MetaManager) ScanTopics(cursor string, limit int) (topics []string, err error) {
	return
}
