package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	ckpt "github.com/OpenAtomFoundation/pikiwidb-port-kafka-go/internal/checkpoint"
	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ProducerConfig struct {
	Brokers           string
	ClientID          string
	MessageMaxBytes   int
	EnableIdempotence bool
}

type Record struct {
	Topic string
	Key   []byte
	Value []byte

	HasAck bool
	Ack    ckpt.Position
	Epoch  uint32

	HasSnapshotAck bool
	SnapshotSeq    uint64
}

type deliveryContext struct {
	hasAck bool
	epoch  uint32
	pos    ckpt.Position

	hasSnapshot bool
	snapshotSeq uint64
}

type Producer struct {
	p    *ckafka.Producer
	log  *log.Logger
	ckpt *ckpt.Manager
}

func NewProducer(cfg ProducerConfig, ckptMgr *ckpt.Manager, logger *log.Logger) (*Producer, error) {
	if logger == nil {
		logger = log.Default()
	}
	if cfg.Brokers == "" {
		return nil, fmt.Errorf("kafka brokers required")
	}
	conf := &ckafka.ConfigMap{
		"bootstrap.servers":                     cfg.Brokers,
		"client.id":                             cfg.ClientID,
		"acks":                                  "all",
		"enable.idempotence":                    cfg.EnableIdempotence,
		"max.in.flight.requests.per.connection": 1,
	}
	if cfg.MessageMaxBytes > 0 {
		_ = conf.SetKey("message.max.bytes", cfg.MessageMaxBytes)
	}
	p, err := ckafka.NewProducer(conf)
	if err != nil {
		return nil, err
	}
	return &Producer{
		p:    p,
		log:  logger,
		ckpt: ckptMgr,
	}, nil
}

func (p *Producer) Raw() *ckafka.Producer { return p.p }

func (p *Producer) Flush(timeout time.Duration) int {
	if p == nil || p.p == nil {
		return 0
	}
	ms := int(timeout.Milliseconds())
	if ms <= 0 {
		ms = int((10 * time.Second).Milliseconds())
	}
	return p.p.Flush(ms)
}

func (p *Producer) Close() {
	if p == nil || p.p == nil {
		return
	}
	_ = p.Flush(10 * time.Second)
	p.p.Close()
}

func (p *Producer) Produce(r Record) error {
	if p == nil || p.p == nil {
		return fmt.Errorf("producer not initialized")
	}
	topic := r.Topic
	msg := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		Key:            r.Key,
		Value:          r.Value,
	}
	if r.HasAck || r.HasSnapshotAck {
		ctx := &deliveryContext{}
		if r.HasAck {
			ctx.hasAck = true
			ctx.epoch = r.Epoch
			ctx.pos = r.Ack
		}
		if r.HasSnapshotAck {
			ctx.hasSnapshot = true
			ctx.snapshotSeq = r.SnapshotSeq
		}
		msg.Opaque = ctx
	}
	for {
		err := p.p.Produce(msg, nil)
		if err == nil {
			return nil
		}
		var kerr ckafka.Error
		if errors.As(err, &kerr) && kerr.Code() == ckafka.ErrQueueFull {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return err
	}
}

// RunDeliveryLoop drains librdkafka delivery reports and advances checkpoint
// only when the message delivery succeeded.
func (p *Producer) RunDeliveryLoop(ctx context.Context) {
	if p == nil || p.p == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-p.p.Events():
			switch e := ev.(type) {
			case *ckafka.Message:
				if e.TopicPartition.Error != nil {
					if dc, ok := e.Opaque.(*deliveryContext); ok && dc != nil && (dc.hasAck || dc.hasSnapshot) {
						p.log.Printf("kafka delivery failed topic=%s key_len=%d value_len=%d err=%v",
							*e.TopicPartition.Topic, len(e.Key), len(e.Value), e.TopicPartition.Error)
					}
					continue
				}
				dc, ok := e.Opaque.(*deliveryContext)
				if !ok || dc == nil || p.ckpt == nil {
					continue
				}
				if dc.hasAck {
					if err := p.ckpt.OnDone(dc.epoch, dc.pos); err != nil {
						p.log.Printf("checkpoint OnDone failed: %v", err)
					}
				}
				if dc.hasSnapshot {
					if err := p.ckpt.OnSnapshotDone(dc.snapshotSeq); err != nil {
						p.log.Printf("checkpoint OnSnapshotDone failed: %v", err)
					}
				}
			case ckafka.Error:
				p.log.Printf("kafka error: %v", e)
			default:
			}
		}
	}
}
