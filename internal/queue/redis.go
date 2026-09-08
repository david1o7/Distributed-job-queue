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
	MainDepth    int64 `json:"main_depth"`
	ReadyHigh    int64 `json:"ready_high"`
	ReadyDefault int64 `json:"ready_default"`
	ReadyLow     int64 `json:"ready_low"`
	ReadyLegacy  int64 `json:"ready_legacy,omitempty"`
	Processing   int64 `json:"processing"`
	Delayed      int64 `json:"delayed"`
	DeadLetter   int64 `json:"dead_letter"`
}

type DelayedJobView struct {
	Job     jobs.Job `json:"job"`
	ReadyAt int64    `json:"ready_at"`
	ReadyIn int64    `json:"ready_in"`
	Orphan  bool     `json:"orphan,omitempty"`
}

type DeadJobPage struct {
	Items   []jobs.DeadJob `json:"items"`
	Total   int64          `json:"total"`
	Start   int64          `json:"start"`
	Count   int64          `json:"count"`
	HasMore bool           `json:"has_more"`
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

	ListDeadJobsPage(ctx context.Context, start, count int64) (DeadJobPage, error)

	ListDelayedJobs(ctx context.Context) ([]DelayedJobView, error)
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

// Chabge above
func NewRedisQueue(addr string) *RedisQueue {
	client := redis.NewClient(
		&redis.Options{Addr: addr})

	return &RedisQueue{
		Client: client,
	}
}

func (q *RedisQueue) Push(ctx context.Context, job jobs.Job) error {
	job.Priority = jobs.NormalizePriority(job.Priority)
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	dest := jobs.QueueKeyFor(job.Priority)
	return q.Client.LPush(ctx, dest, data).Err()
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
	dest := jobs.QueueKeyFor(job.Priority)
	res, err := replayDeadJobScript.Run(
		ctx,
		q.Client,
		[]string{"dead_job", dest},
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
	keys := jobs.AllReadyQueues()

	result, err := q.Client.BRPop(ctx, 0, keys...).Result()

	if err != nil {
		return nil, err
	}

	var job jobs.Job

	if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
		return nil, err
	}

	job.Priority = jobs.NormalizePriority(job.Priority)

	deadline := time.Now().Add(visibilityTimeout).Unix()

	_, err = claimAddToProcessingScript.Run(
		ctx,
		q.Client,
		[]string{"jobs:processing"},
		job.ID,
		deadline,
	).Result()

	if err != nil {
		_ = q.Client.LPush(ctx, jobs.QueueKeyFor(job.Priority), result[1])
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
	dest := jobs.QueueKeyFor(job.Priority)
	pipe := q.Client.Pipeline()
	pipe.ZRem(ctx, "jobs:processing", job.ID)
	pipe.LPush(ctx, dest, data)
	_, err = pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) ReapExpired(ctx context.Context) (int, error) {
	now := float64(time.Now().Unix())

	ids, err := q.Client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     "jobs:processing",
		Start:   "-inf",
		Stop:    now,
		ByScore: true,
	}).Result()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, id := range ids {
		job, err := q.GetJob(ctx, id)
		if err != nil || job == nil {
			_ = q.Client.ZRem(ctx, "jobs:processing", id)
			continue
		}

		job.Priority = jobs.NormalizePriority(job.Priority)
		payload, err := json.Marshal(job)
		if err != nil {
			continue
		}

		dest := jobs.QueueKeyFor(job.Priority)
		moved, err := moveOneExpiredScript.Run(
			ctx,
			q.Client,
			[]string{"jobs:processing", dest},
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
		if err != nil || job == nil {
			_ = q.Client.ZRem(ctx, "jobs:delayed", id)
			continue
		}

		job.Status = jobs.StatusQueued

		payload, err := json.Marshal(job)
		if err != nil {
			continue
		}

		_ = q.SaveJob(ctx, *job)

		dest := jobs.QueueKeyFor(job.Priority)
		moved, err := moveOneDelayedScript.Run(
			ctx,
			q.Client,
			[]string{"jobs:delayed", dest},
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

	highCh := pipe.LLen(ctx, "jobs:high")
	defaultCh := pipe.LLen(ctx, "jobs:default")
	lowCh := pipe.LLen(ctx, "jobs:low")
	legacyCh := pipe.LLen(ctx, "jobs")
	procCh := pipe.ZCard(ctx, "jobs:processing")
	delayedCh := pipe.ZCard(ctx, "jobs:delayed")
	deadCh := pipe.LLen(ctx, "dead_job")

	if _, err := pipe.Exec(ctx); err != nil {
		return QueueStats{}, err
	}

	high := highCh.Val()
	def := defaultCh.Val()
	low := lowCh.Val()
	legacy := legacyCh.Val()

	return QueueStats{
		MainDepth:    high + def + low + legacy,
		ReadyHigh:    high,
		ReadyDefault: def,
		ReadyLow:     low,
		ReadyLegacy:  legacy,
		Processing:   procCh.Val(),
		Delayed:      delayedCh.Val(),
		DeadLetter:   deadCh.Val(),
	}, nil
}

func (q *RedisQueue) ListDelayedJobs(ctx context.Context) ([]DelayedJobView, error) {
	pairs, err := q.Client.ZRangeWithScores(ctx, "jobs:delayed", 0, -1).Result()
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	out := make([]DelayedJobView, 0, len(pairs))
	for _, z := range pairs {
		id, _ := z.Member.(string)
		readyAt := int64(z.Score)
		readyIn := readyAt - now
		if readyIn < 0 {
			readyIn = 0
		}
		job, err := q.GetJob(ctx, id)
		if err != nil || job == nil {
			out = append(out, DelayedJobView{
				Job:     jobs.Job{ID: id},
				ReadyAt: readyAt,
				ReadyIn: readyIn,
				Orphan:  true,
			})
			continue
		}
		out = append(out, DelayedJobView{
			Job:     *job,
			ReadyAt: readyAt,
			ReadyIn: readyIn,
		})
	}
	return out, nil
}

func (q *RedisQueue) ListDeadJobsPage(ctx context.Context, start, count int64) (DeadJobPage, error) {
	if start < 0 {
		start = 0
	}
	if count <= 0 {
		count = 50
	}
	if count > 100 {
		count = 100
	}

	total, err := q.Client.LLen(ctx, "dead_job").Result()
	if err != nil {
		return DeadJobPage{}, err
	}

	end := start + count - 1
	values, err := q.Client.LRange(ctx, "dead_job", start, end).Result()
	if err != nil {
		return DeadJobPage{}, err
	}

	items := make([]jobs.DeadJob, 0, len(values))
	for _, v := range values {
		var d jobs.DeadJob
		if err := json.Unmarshal([]byte(v), &d); err != nil {
			continue
		}
		items = append(items, d)
	}

	return DeadJobPage{
		Items:   items,
		Total:   total,
		Start:   start,
		Count:   int64(len(items)),
		HasMore: start+int64(len(items)) < total,
	}, nil
}
