package rabbit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"distributed-job-system/internal/jobs"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	qHigh    = "jobs.high"
	qDefault = "jobs.default"
	qLow     = "jobs.low"
	qWait    = "jobs.wait"
	qDLQ     = "jobs.dlq"
)

type Broker struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	mu       sync.Mutex
	inflight map[string]amqp.Delivery
}

func New(url string) (*Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbit dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbit channel: %w", err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	b := &Broker{
		conn:     conn,
		ch:       ch,
		inflight: make(map[string]amqp.Delivery),
	}
	if err := b.declareTopology(); err != nil {
		_ = b.Close(context.Background())
		return nil, err
	}
	return b, nil
}

func (b *Broker) declareTopology() error {
	// Ready queues
	for _, name := range []string{qHigh, qDefault, qLow, qDLQ} {
		if _, err := b.ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare %s: %w", name, err)
		}
	}

	waitArgs := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": qDefault,
	}
	if _, err := b.ch.QueueDeclare(qWait, true, false, false, false, waitArgs); err != nil {
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

func (b *Broker) Push(ctx context.Context, job jobs.Job) error {
	job.Priority = jobs.NormalizePriority(job.Priority)
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return b.ch.PublishWithContext(ctx, "", queueFor(job.Priority), false, false, amqp.Publishing{
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
}

func (b *Broker) Claim(ctx context.Context, visibilityTimeout time.Duration) (*jobs.Job, error) {
	_ = visibilityTimeout

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		for _, qn := range []string{qHigh, qDefault, qLow} {
			d, ok, err := b.ch.Get(qn, false) // autoAck=false
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}

			var job jobs.Job
			if err := json.Unmarshal(d.Body, &job); err != nil {
				// Poison: do not requeue
				_ = d.Nack(false, false)
				return nil, fmt.Errorf("invalid job payload: %w", err)
			}
			job.Priority = jobs.NormalizePriority(job.Priority)

			b.mu.Lock()
			b.inflight[job.ID] = d
			b.mu.Unlock()

			return &job, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

func (b *Broker) ACK(ctx context.Context, jobID string) error {
	b.mu.Lock()
	d, ok := b.inflight[jobID]
	if ok {
		delete(b.inflight, jobID)
	}
	b.mu.Unlock()
	if !ok {
		return nil
	}
	return d.Ack(false)
}

func (b *Broker) Nack(ctx context.Context, job jobs.Job) error {
	b.mu.Lock()
	d, ok := b.inflight[job.ID]
	if ok {
		delete(b.inflight, job.ID)
	}
	b.mu.Unlock()
	if !ok {
		return b.Push(ctx, job)
	}
	return d.Nack(false, true)
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

	exp := fmt.Sprintf("%d", delay.Milliseconds())
	return b.ch.PublishWithContext(ctx, "", qWait, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    job.ID,
		Body:         body,
		Expiration:   exp,
		Headers: amqp.Table{
			"job_type": string(job.Type),
			"priority": string(job.Priority),
		},
	})
}

func (b *Broker) MoveToDeadLetter(ctx context.Context, dead jobs.DeadJob) error {
	_ = b.ACK(ctx, dead.ID)
	body, err := json.Marshal(dead)
	if err != nil {
		return err
	}
	return b.ch.PublishWithContext(ctx, "", qDLQ, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    dead.ID,
		Body:         body,
	})
}

func (b *Broker) Close(ctx context.Context) error {
	b.mu.Lock()
	b.inflight = map[string]amqp.Delivery{}
	b.mu.Unlock()
	if b.ch != nil {
		_ = b.ch.Close()
	}
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

func (b *Broker) ReplayDeadJob(ctx context.Context, id string) (*jobs.Job, error) {
	for {
		d, ok, err := b.ch.Get(qDLQ, false)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("not found")
		}
		var dead jobs.DeadJob
		if err := json.Unmarshal(d.Body, &dead); err != nil {
			_ = d.Nack(false, false)
			continue
		}
		if dead.ID != id {

			_ = d.Nack(false, true)
			return nil, fmt.Errorf("not found in head scan; improve with index")
		}
		_ = d.Ack(false)
		job := dead.Job
		job.Status = jobs.StatusQueued
		job.RetryCount = 0
		if err := b.Push(ctx, job); err != nil {
			return nil, err
		}
		return &job, nil
	}
}
