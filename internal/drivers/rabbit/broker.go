package rabbit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	qHigh    = "jobs.high"
	qDefault = "jobs.default"
	qLow     = "jobs.low"
	qWait    = "jobs.wait"
	qDLQ     = "jobs.dlq"

	// Parallel AMQP ops. Raise if CPU/network allow (32–64).
	defaultPoolSize = 32
)

type inFlight struct {
	d  amqp.Delivery
	ch *amqp.Channel // ACK/Nack MUST use this channel
}

type Broker struct {
	url      string
	poolSize int

	connMu sync.Mutex
	conn   *amqp.Connection

	pool chan *amqp.Channel

	flightMu sync.Mutex
	inflight map[string]inFlight
}

func New(url string) (*Broker, error) {
	return NewWithPool(url, defaultPoolSize)
}

func NewWithPool(url string, poolSize int) (*Broker, error) {
	if poolSize < 8 {
		poolSize = 8
	}
	b := &Broker{
		url:      url,
		poolSize: poolSize,
		pool:     make(chan *amqp.Channel, poolSize),
		inflight: make(map[string]inFlight),
	}
	if err := b.reconnect(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) reconnect() error {
	b.connMu.Lock()
	defer b.connMu.Unlock()

	// Drop old pool channels
	old := b.pool
	b.pool = make(chan *amqp.Channel, b.poolSize)
	if old != nil {
		go drainAndClose(old)
	}
	if b.conn != nil {
		_ = b.conn.Close()
		b.conn = nil
	}

	conn, err := amqp.DialConfig(b.url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("rabbit dial: %w", err)
	}
	b.conn = conn

	go func(c *amqp.Connection) {
		e := <-c.NotifyClose(make(chan *amqp.Error, 1))
		if e != nil {
			logger.Log.Error("rabbit connection closed", "error", e)
		}
		b.connMu.Lock()
		if b.conn == c {
			b.conn = nil
		}
		b.connMu.Unlock()
	}(conn)

	setup, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("rabbit setup channel: %w", err)
	}
	if err := declareTopology(setup); err != nil {
		_ = setup.Close()
		_ = conn.Close()
		return err
	}
	_ = setup.Close()

	for i := 0; i < b.poolSize; i++ {
		ch, err := conn.Channel()
		if err != nil {
			return fmt.Errorf("rabbit pool channel: %w", err)
		}
		if err := ch.Qos(1, 0, false); err != nil {
			_ = ch.Close()
			return err
		}
		b.pool <- ch
	}
	logger.Log.Info("rabbit pool ready", "channels", b.poolSize)
	return nil
}

func drainAndClose(pool chan *amqp.Channel) {
	for {
		select {
		case ch, ok := <-pool:
			if !ok {
				return
			}
			if ch != nil {
				_ = ch.Close()
			}
		default:
			return
		}
	}
}

func declareTopology(ch *amqp.Channel) error {
	for _, name := range []string{qHigh, qDefault, qLow, qDLQ} {
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare %s: %w", name, err)
		}
	}
	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": qDefault,
	}
	if _, err := ch.QueueDeclare(qWait, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare %s: %w", qWait, err)
	}
	return nil
}

func queueFor(p jobs.Priority) string {
	switch jobs.NormalizePriority(p) {
	case jobs.PriorityHigh:
		return qHigh
	case jobs.PriorityLow:
		return qLow
	default:
		return qDefault
	}
}

func (b *Broker) borrow(ctx context.Context) (*amqp.Channel, error) {
	for {
		b.connMu.Lock()
		dead := b.conn == nil || b.conn.IsClosed()
		b.connMu.Unlock()
		if dead {
			if err := b.reconnect(); err != nil {
				return nil, err
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case ch, ok := <-b.pool:
			if !ok {
				return nil, fmt.Errorf("rabbit pool closed")
			}
			return ch, nil
		}
	}
}

func (b *Broker) release(ch *amqp.Channel) {
	if ch == nil {
		return
	}
	select {
	case b.pool <- ch:
	default:
		_ = ch.Close()
	}
}

func (b *Broker) Push(ctx context.Context, job jobs.Job) error {
	job.Priority = jobs.NormalizePriority(job.Priority)
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	ch, err := b.borrow(ctx)
	if err != nil {
		return err
	}
	
	err = ch.PublishWithContext(ctx, "", queueFor(job.Priority), false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    job.ID,
		Timestamp:    time.Now().UTC(),
		Body:         body,
		Headers: amqp.Table{
			"job_type": string(job.Type),
			"priority": string(job.Priority),
		},
	})
	if err != nil {
		_ = ch.Close() // do not recycle a broken channel
		return err
	}
	b.release(ch)
	return nil
}

func (b *Broker) Claim(ctx context.Context, visibilityTimeout time.Duration) (*jobs.Job, error) {
	_ = visibilityTimeout

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		ch, err := b.borrow(ctx)
		if err != nil {
			return nil, err
		}

		var (
			d  amqp.Delivery
			ok bool
		)
		for _, qn := range []string{qHigh, qDefault, qLow} {
			d, ok, err = ch.Get(qn, false)
			if err != nil {
				_ = ch.Close()
				return nil, err
			}
			if ok {
				break
			}
		}

		if !ok {
			b.release(ch)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}

		var job jobs.Job
		if err := json.Unmarshal(d.Body, &job); err != nil {
			_ = d.Nack(false, false)
			b.release(ch)
			return nil, fmt.Errorf("invalid job payload: %w", err)
		}
		job.Priority = jobs.NormalizePriority(job.Priority)

		// Keep channel until ACK/Nack (same-channel rule).
		b.flightMu.Lock()
		b.inflight[job.ID] = inFlight{d: d, ch: ch}
		b.flightMu.Unlock()
		return &job, nil
	}
}

func (b *Broker) ACK(ctx context.Context, jobID string) error {
	b.flightMu.Lock()
	ref, ok := b.inflight[jobID]
	if ok {
		delete(b.inflight, jobID)
	}
	b.flightMu.Unlock()
	if !ok {
		return nil
	}
	err := ref.d.Ack(false)
	b.release(ref.ch)
	return err
}

func (b *Broker) Nack(ctx context.Context, job jobs.Job) error {
	b.flightMu.Lock()
	ref, ok := b.inflight[job.ID]
	if ok {
		delete(b.inflight, job.ID)
	}
	b.flightMu.Unlock()
	if !ok {
		return b.Push(ctx, job)
	}
	err := ref.d.Nack(false, true)
	b.release(ref.ch)
	return err
}

func (b *Broker) Schedule(ctx context.Context, job jobs.Job, delay time.Duration) error {
	if delay < time.Second {
		delay = time.Second
	}
	_ = b.ACK(ctx, job.ID)

	job.Status = jobs.StatusRetrying
	job.Priority = jobs.NormalizePriority(job.Priority)
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	ch, err := b.borrow(ctx)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(ctx, "", qWait, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    job.ID,
		Body:         body,
		Expiration:   fmt.Sprintf("%d", delay.Milliseconds()),
		Headers: amqp.Table{
			"job_type": string(job.Type),
			"priority": string(job.Priority),
		},
	})
	if err != nil {
		_ = ch.Close()
		return err
	}
	b.release(ch)
	return nil
}

func (b *Broker) MoveToDeadLetter(ctx context.Context, dead jobs.DeadJob) error {
	id := dead.ID
	if id == "" {
		id = dead.ID
	}
	_ = b.ACK(ctx, id)

	body, err := json.Marshal(dead)
	if err != nil {
		return err
	}
	ch, err := b.borrow(ctx)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(ctx, "", qDLQ, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    id,
		Body:         body,
	})
	if err != nil {
		_ = ch.Close()
		return err
	}
	b.release(ch)
	return nil
}

func (b *Broker) Close(ctx context.Context) error {
	b.flightMu.Lock()
	b.inflight = map[string]inFlight{}
	b.flightMu.Unlock()

	b.connMu.Lock()
	defer b.connMu.Unlock()

	drainAndClose(b.pool)
	if b.conn != nil {
		err := b.conn.Close()
		b.conn = nil
		return err
	}
	return nil
}