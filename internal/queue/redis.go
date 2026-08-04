package queue

import (
	"context"
	"distributed-job-system/internal/jobs"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)
type Queue interface {

    Push(ctx context.Context, job jobs.Job) error

    Pop(ctx context.Context) (*jobs.Job, error)

    SaveJob(ctx context.Context, job jobs.Job) error

    GetJob(ctx context.Context, id string) (*jobs.Job, error)
}

type RedisQueue struct{
	client *redis.Client
}

func NewRedisQueue(addr string) *RedisQueue {
	client := redis.NewClient(
		&redis.Options{ Addr: addr,})

	return &RedisQueue{
		client: client,
	}
}

func (q *RedisQueue) Push(ctx context.Context , job jobs.Job) error {
	data, err := json.Marshal(job)
	if err != nil{
		return err
	}

	return q.client.LPush(ctx, "jobs", data).Err()
}

func (q *RedisQueue) Pop(ctx context.Context) (*jobs.Job, error){
	result, err := q.client.BRPop(ctx, 0, "jobs").Result()

	if err != nil{
		return nil, err
	}

	var job jobs.Job

	err = json.Unmarshal([]byte(result[1]), &job)
	if err != nil{
		return nil, err
	}

	return &job, nil
}

func (q *RedisQueue) SaveJob(ctx context.Context, job jobs.Job) error {
	data, err := json.Marshal(job)

	if err != nil{
		return err
	}

	return q.client.Set(
		ctx,
		"job:"+ job.ID,
		data,
		24*time.Hour,).Err()

	
}

func (q *RedisQueue) GetJob(ctx context.Context, id string,) (*jobs.Job, error) {

    val, err := q.client.Get(
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