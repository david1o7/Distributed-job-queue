package queue

import (
	"context"
	"distributed-job-system/internal/jobs"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const processedSetKey = "jobs:processed"
const processedTTL = 24 * time.Hour
const concurrencyKeyPrefix = "jobs:concurrency:"
type QueueStats struct {
	MainDepth    int64
	Processing   int64
	Delayed      int64
	DeadLetter   int64
}

type Queue interface {
	Push(ctx context.Context, job jobs.Job) error

	Pop(ctx context.Context) (*jobs.Job, error)

	SaveJob(ctx context.Context, job jobs.Job) error

	GetJob(ctx context.Context, id string) (*jobs.Job, error)

	MoveToDeadLetter(ctx context.Context, job jobs.DeadJob) error

	ListDeadJobs(ctx context.Context) ([]jobs.DeadJob, error)

	ReplayDeadJob(ctx context.Context, id string) (*jobs.Job, error)

	Claim(ctx context.Context, visibilityTimeout time.Duration) (*jobs.Job, error)

	ACK(ctx context.Context, jobID string) error

	Nack(ctx context.Context, job jobs.Job) error

	ReapExpired(ctx context.Context) (int, error)

	Schedule(ctx context.Context, job jobs.Job, delay time.Duration) error

	MoveReadyDelayedJobs(ctx context.Context) (int, error)

	ExtendVisibility(ctx context.Context, jobID string, extension time.Duration) error

	Ping(ctx context.Context) error

	IsProcessed(ctx context.Context, key string) (bool, error)

	MarkProcessed(ctx context.Context, key string) error

	AcquireConcurrency(ctx context.Context, jobType string, limit int) (bool, error)

	ReleaseConcurrency(ctx context.Context, jobType string) error

	Stats(ctx context.Context) (QueueStats, error)
}

var acquireConcurrencyScript = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

local current = tonumber(redis.call("GET", key) or "0")
if current >= limit then
  return 0
end

current = redis.call("INCR", key)
-- Safety TTL so a crashed worker doesn't hold the slot forever
redis.call("EXPIRE", key, ttl)
return 1
`)

var releaseConcurrencyScript = redis.NewScript(`
local key = KEYS[1]
local current = tonumber(redis.call("GET", key) or "0")
if current <= 0 then
  redis.call("SET", key, 0)
  return 0
end
return redis.call("DECR", key)
`)

var claimAddToProcessingScript = redis.NewScript(`
local processingKey = KEYS[1]
local jobID = ARGV[1]
local score = ARGV[2]

redis.call("ZADD", processingKey, score, jobID)
return 1
`)

var extendVisibilityScript = redis.NewScript(`
local key = KEYS[1]
local member = ARGV[1]
local newScore = ARGV[2]

local exists = redis.call("ZSCORE", key, member)
if not exists then
  return 0
end

redis.call("ZADD", key, newScore, member)
return 1
`)

var ackScript = redis.NewScript(`
return redis.call("ZREM", KEYS[1], ARGV[1])
`)

var scheduleAndRemoveFromProcessingScript = redis.NewScript(`
local delayedKey = KEYS[1]
local processingKey = KEYS[2]
local jobID = ARGV[1]
local score = ARGV[2]

redis.call("ZADD", delayedKey, score, jobID)
redis.call("ZREM", processingKey, jobID)
return 1
`)

var moveOneDelayedScript = redis.NewScript(`
local delayedKey = KEYS[1]
local mainQueueKey = KEYS[2]
local jobID = ARGV[1]
local jobPayload = ARGV[2]

local removed = redis.call("ZREM", delayedKey, jobID)
if removed == 0 then
  return 0
end

redis.call("LPUSH", mainQueueKey, jobPayload)
return 1
`)

var moveOneExpiredScript = redis.NewScript(`
local processingKey = KEYS[1]
local mainQueueKey = KEYS[2]
local jobID = ARGV[1]
local jobPayload = ARGV[2]

local removed = redis.call("ZREM", processingKey, jobID)
if removed == 0 then
  return 0
end

redis.call("LPUSH", mainQueueKey, jobPayload)
return 1
`)

var replayDeadJobScript = redis.NewScript(`
local dlqKey = KEYS[1]
local mainKey = KEYS[2]
local targetID = ARGV[1]
local cleaned = ARGV[2]

local items = redis.call("LRANGE", dlqKey, 0, -1)

for _, item in ipairs(items) do
  -- plain find (no patterns); support both compact and spaced JSON
  if string.find(item, '"id":"' .. targetID .. '"', 1, true)
     or string.find(item, '"id": "' .. targetID .. '"', 1, true) then
    redis.call("LREM", dlqKey, 1, item)
    redis.call("LPUSH", mainKey, cleaned)
    return item
  end
end

return false
`)

type RedisQueue struct {
	Client *redis.Client
}


func (q *RedisQueue) IsProcessed(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, nil
	}
	return q.Client.SIsMember(ctx, processedSetKey, key).Result()
}


func (q *RedisQueue) MarkProcessed(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}

	pipe := q.Client.Pipeline()
	pipe.SAdd(ctx, processedSetKey, key)
	pipe.Expire(ctx, processedSetKey, processedTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) Ping(ctx context.Context) error {
	return q.Client.Ping(ctx).Err()
}
//Chabge above
func NewRedisQueue(addr string) *RedisQueue {
	client := redis.NewClient(
		&redis.Options{Addr: addr})

	return &RedisQueue{
		Client: client,
	}
}

func (q *RedisQueue) Push(ctx context.Context, job jobs.Job) error {
	
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return q.Client.LPush(ctx, "jobs", data).Err()
}

func (q *RedisQueue) Pop(ctx context.Context) (*jobs.Job, error) {
	result, err := q.Client.BRPop(ctx, 0, "jobs").Result()

	if err != nil {
		return nil, err
	}

	var job jobs.Job

	err = json.Unmarshal([]byte(result[1]), &job)
	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (q *RedisQueue) SaveJob(ctx context.Context, job jobs.Job) error {
	data, err := json.Marshal(job)

	if err != nil {
		return err
	}

	return q.Client.Set(
		ctx,
		"job:"+job.ID,
		data,
		24*time.Hour).Err()

}

func (q *RedisQueue) GetJob(ctx context.Context, id string) (*jobs.Job, error) {

	val, err := q.Client.Get(
		ctx,
		"job:"+id,
	).Result()

	if err != nil {
		return nil, err
	}

	var job jobs.Job

	if err := json.Unmarshal(
		[]byte(val),
		&job,
	); err != nil {

		return nil, err
	}

	return &job, nil
}

func (q *RedisQueue) MoveToDeadLetter(ctx context.Context, job jobs.DeadJob) error {
	data, err := json.Marshal(job)

	if err != nil {
		return err
	}

	return q.Client.LPush(
		ctx,
		"dead_job",
		data,
	).Err()

}

func (q *RedisQueue) ListDeadJobs(ctx context.Context) ([]jobs.DeadJob, error) {
	values, err := q.Client.LRange(
		ctx,
		"dead_job",
		0,
		-1,
	).Result()

	if err != nil {
		return nil, err
	}

	deadjobs := make([]jobs.DeadJob, 0, len(values))

	for _, value := range values {
		var job jobs.DeadJob

		if err := json.Unmarshal([]byte(value), &job); err != nil {
			continue
		}

		deadjobs = append(deadjobs, job)
	}

	return deadjobs, nil
}

func (q *RedisQueue) ReplayDeadJob(ctx context.Context, id string) (*jobs.Job, error) {
	if id == "" {
		return nil, redis.Nil
	}

	values, err := q.Client.LRange(ctx, "dead_job", 0, -1).Result()
	if err != nil {
		return nil, err
	}

	var target *jobs.DeadJob
	for _, value := range values {
		var deadJob jobs.DeadJob
		if err := json.Unmarshal([]byte(value), &deadJob); err != nil {
			continue
		}
		if deadJob.ID == id {
			target = &deadJob
			break
		}
	}
	if target == nil {
		return nil, redis.Nil
	}

	job := target.Job
	job.Status = jobs.StatusQueued
	job.RetryCount = 0
	job.NextRetry = time.Time{}
	
	job.StartedAt = time.Time{}
	job.FinishedAt = time.Time{}
	job.WorkerID = 0

	cleaned, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}

	res, err := replayDeadJobScript.Run(
		ctx,
		q.Client,
		[]string{"dead_job", "jobs"},
		id,
		string(cleaned),
	).Result()

	if err == redis.Nil {
		return nil, redis.Nil
	}
	if err != nil {
		return nil, err
	}

	if res == nil || res == false {
		return nil, redis.Nil
	}

	if err := q.SaveJob(ctx, job); err != nil {
		return &job, fmt.Errorf("replayed but SaveJob failed: %w", err)
	}

	return &job, nil
}

func (q *RedisQueue) Claim(ctx context.Context, visibilityTimeout time.Duration) (*jobs.Job, error) {
	result, err := q.Client.BRPop(ctx, 0, "jobs").Result()

	if err != nil {
		return nil, err
	}

	var job jobs.Job

	if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(visibilityTimeout).Unix()

	_, err = claimAddToProcessingScript.Run(
		ctx,
		q.Client,
		[]string{"jobs:processing"},
		job.ID,
		deadline,
	).Result()

	if err != nil {
		_ = q.Client.LPush(ctx, "jobs", result[1])

		return nil, err
	}

	return &job, nil
}

func (q *RedisQueue) ACK(ctx context.Context, jobID string) error {

	_, err := ackScript.Run(
		ctx,
		q.Client,
		[]string{"jobs:processing"},
		jobID,
	).Result()

	return err
}

func (q *RedisQueue) ExtendVisibility(ctx context.Context, jobID string, extension time.Duration) error {
	newScore := time.Now().Add(extension).Unix()

	_, err := extendVisibilityScript.Run(
		ctx,
		q.Client,
		[]string{"jobs:processing"},
		jobID,
		newScore,
	).Result()

	return err
}

func (q *RedisQueue) Nack(ctx context.Context, job jobs.Job) error {
	data, err := json.Marshal(job)

	if err != nil {
		return err
	}

	pipe := q.Client.Pipeline()
	pipe.ZRem(ctx, "jobs:processing", job.ID)
	pipe.LPush(ctx, "jobs", data)
	_, err = pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) ReapExpired(ctx context.Context) (int, error) {
	now := float64(time.Now().Unix())

	ids, err := q.Client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     "jobs:processing",
		Start:   "-inf",
		Stop:    fmt.Sprintf("%f", now),
		ByScore: true,
	}).Result()

	if err != nil {
		return 0, err
	}
	count := 0
	for _, id := range ids {
		job, err := q.GetJob(ctx, id)
		if err != nil {
			_ = q.Client.ZRem(ctx, "jobs:processing", id)
			continue
		}

		payload, err := json.Marshal(job)
		if err != nil {
			continue
		}

		moved, err := moveOneExpiredScript.Run(
			ctx,
			q.Client,
			[]string{"jobs:processing", "jobs"},
			id,
			string(payload),
		).Int()

		if err == nil && moved == 1 {
			count++
		}
	}
	return count, nil
}

func (q *RedisQueue) Schedule(ctx context.Context, job jobs.Job, delay time.Duration) error {
	readyAt := time.Now().Add(delay).Unix()

	if err := q.SaveJob(ctx, job); err != nil {
		return err
	}

	_, err := scheduleAndRemoveFromProcessingScript.Run(
		ctx,
		q.Client,
		[]string{"jobs:delayed", "jobs:processing"},
		job.ID,
		readyAt,
	).Result()

	return err
}

func (q *RedisQueue) MoveReadyDelayedJobs(ctx context.Context) (int, error) {
	now := float64(time.Now().Unix())

	ids, err := q.Client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     "jobs:delayed",
		Start:   "-inf",
		Stop:    fmt.Sprintf("%f", now),
		ByScore: true,
	}).Result()

	if err != nil {
		return 0, err
	}

	movedCount := 0
	for _, id := range ids {
		job, err := q.GetJob(ctx, id)
		if err != nil {

			_ = q.Client.ZRem(ctx, "jobs:delayed", id)
			continue
		}

		job.Status = jobs.StatusQueued

		payload, err := json.Marshal(job)
		if err != nil {
			continue
		}

		_ = q.SaveJob(ctx, *job)

		moved, err := moveOneDelayedScript.Run(
			ctx,
			q.Client,
			[]string{"jobs:delayed", "jobs"},
			id,
			string(payload),
		).Int()

		if err == nil && moved == 1 {
			movedCount++
		}
	}

	return movedCount, nil
}

func (q *RedisQueue) AcquireConcurrency(ctx context.Context, jobType string, limit int) (bool, error) {
	if limit <= 0 {
		return true, nil 
	}

	key := concurrencyKeyPrefix + jobType
	
	const safetyTTLSeconds = 300

	result, err := acquireConcurrencyScript.Run(
		ctx,
		q.Client,
		[]string{key},
		limit,
		safetyTTLSeconds,
	).Int()

	if err != nil {
		return false, err
	}
	return result == 1, nil
}


func (q *RedisQueue) ReleaseConcurrency(ctx context.Context, jobType string) error {
	key := concurrencyKeyPrefix + jobType
	_, err := releaseConcurrencyScript.Run(ctx, q.Client, []string{key}).Result()
	return err
}

func (q *RedisQueue) Stats(ctx context.Context) (QueueStats, error) {
	pipe := q.Client.Pipeline()
	mainCh := pipe.LLen(ctx, "jobs")
	procCh := pipe.ZCard(ctx, "jobs:processing")
	delayedCh := pipe.ZCard(ctx, "jobs:delayed")
	deadCh := pipe.LLen(ctx, "dead_job")

	if _, err := pipe.Exec(ctx); err != nil {
		return QueueStats{}, err
	}

	return QueueStats{
		MainDepth:  mainCh.Val(),
		Processing: procCh.Val(),
		Delayed:    delayedCh.Val(),
		DeadLetter: deadCh.Val(),
	}, nil
}