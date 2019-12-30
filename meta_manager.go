package qq

import (
	"fmt"
	"sync"

	"encoding/binary"

	"github.com/zhiqiangxu/util"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type metaManager interface {
	CreateExchange(name string, exType int32) error
	DeleteExchange(name string) error
	GetExchange(name string) *Exchange
	BindExchangeAndConsumeQueue(ex, cq, routingKey string) error
	GetOrCreateConsumeQueue(name string) *ConsumeQueue
	ScanConsumeQueues(cursor string, limit int) ([]string, error)
}

var _ metaManager = (*MetaManager)(nil)

// MetaManager for qq
type MetaManager struct {
	exMu   sync.RWMutex
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

func key4Ex(name string) string {
	return fmt.Sprintf(prefixForExchange, name)
}

// CreateExchange will create an exchange if not exists
func (mm *MetaManager) CreateExchange(name string, exType int32) (err error) {

	k4Ex := key4Ex(name)
	var v [4]byte
	binary.BigEndian.PutUint32(v[:], uint32(exType))
	err = mm.store.Insert(util.Slice(k4Ex), v[:])
	if err != nil {
		return
	}

	mm.exMu.Lock()
	mm.allEx[name] = NewExchange(exType)
	mm.exMu.Unlock()

	return
}

// DeleteExchange will delete an exchange if exists
func (mm *MetaManager) DeleteExchange(name string) (err error) {
	k4Ex := key4Ex(name)

	err = mm.store.Delete(util.Slice(k4Ex))
	if err != nil {
		return
	}

	mm.exMu.Lock()
	delete(mm.allEx, name)
	mm.exMu.Unlock()
	return nil
}

// GetExchange by name
func (mm *MetaManager) GetExchange(name string) (ex *Exchange) {
	mm.exMu.RLock()
	ex = mm.allEx[name]
	mm.exMu.RUnlock()
	return
}

func key4bind(ex, cq string) string {
	return fmt.Sprintf(prefixForExchangeBindedCQ, ex, cq)
}

// BindExchangeAndConsumeQueue for bind
func (mm *MetaManager) BindExchangeAndConsumeQueue(ex, cq, routingKey string) (err error) {
	k4bind := key4bind(ex, cq)

	err = mm.store.Insert(util.Slice(k4bind), util.Slice(routingKey))
	if err != nil {
		return
	}

	return
}

// GetOrCreateConsumeQueue will create a ConsumeQueue if not exists
func (mm *MetaManager) GetOrCreateConsumeQueue(name string) *ConsumeQueue {
	return nil
}

// ScanConsumeQueues for iterate over consume queues
func (mm *MetaManager) ScanConsumeQueues(cursor string, limit int) (cqs []string, err error) {
	return
}
