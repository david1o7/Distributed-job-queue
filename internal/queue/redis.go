package queue

import (
	"context"
	"distributed-job-system/internal/jobs"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)
type Queue interface {

    Push(ctx context.Context, job jobs.Job) error

    Pop(ctx context.Context) (*jobs.Job, error)

    SaveJob(ctx context.Context, job jobs.Job) error

    GetJob(ctx context.Context, id string) (*jobs.Job, error)

	MoveToDeadLetter(ctx context.Context,job jobs.DeadJob) error

	ListDeadJobs(ctx context.Context) ([]jobs.DeadJob, error)

	ReplayDeadJob(ctx context.Context, id string) (*jobs.Job, error)
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

func (q *RedisQueue) MoveToDeadLetter(ctx context.Context, job jobs.DeadJob) error{
	data, err := json.Marshal(job)

	if err != nil{
		return err
	}

	return q.client.LPush(
		ctx,
		"dead_job",
		data,
	).Err()

}

func (q *RedisQueue) ListDeadJobs(ctx context.Context) ([]jobs.DeadJob, error){
	values, err := q.client.LRange(
		ctx,
		"dead_job",
		0,
		-1,
	).Result()
	
	if err != nil{
		return nil, err
	}

	deadjobs := make([]jobs.DeadJob, 0, len(values))

	for _, value := range values {
		var job jobs.DeadJob

		if err := json.Unmarshal([]byte(value), &job); err != nil{
			continue
		}

		deadjobs = append(deadjobs, job)
	}

	return deadjobs, nil
}

func (q * RedisQueue) ReplayDeadJob(ctx context.Context, id string) (*jobs.Job, error){
	values, err := q.client.LRange(
		ctx,
		"dead_job",
		0,
		-1,
	).Result()

	if err != nil{
		return nil, err
	}

	for _, value := range values{
		var deadJob jobs.DeadJob
		
		if err := json.Unmarshal([]byte(value), &deadJob); err != nil{
			continue
		}

		if deadJob.ID != id{
			continue
		}

		job := deadJob.Job

		job.Status = jobs.StatusQueued
		job.RetryCount = 0
		job.NextRetry = time.Time{}

		if err := q.Push(ctx, job); err != nil{
			return nil, err
		}

		remove, err := q.client.LRem(
			ctx,
			"dead_job",
			1,
			value,
		).Result()

		if err != nil{
			return nil, err
		}

		if remove == 0{
			return nil, fmt.Errorf(
				"job %s could not be removed from DLQ", id,
			)
		}

		if err := q.SaveJob(ctx, job); err != nil{
			return nil, err
		}
		
		return &job, nil
	}

	return nil, redis.Nil
}