package mongo

import (
	"context"
	"time"

	driver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type JobRepository struct {
	client *driver.Client
	coll   *driver.Collection
}

func NewJobRepository(client *driver.Client, coll *driver.Collection) *JobRepository {
	 return &JobRepository{
			client : client,
			coll : coll,
		}
}

func (r *JobRepository) GetJob(ctx context.Context, id string) (*Job, error) {
	filter := bson.M{"_id": id}
	var job Job
	err := r.coll.FindOne(ctx, filter).Decode(&job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *JobRepository) SaveJobs(ctx context.Context, jobs)[Job] error {
	_, err := r.coll.InsertMany(ctx,jobs)
	return err
}

func (r *JobRepository) UpdateJob(ctx context.Context, id string, job *Job) error {
	filter := bson.M{"_id": id}
	_, err := r.coll.UpdateOne(ctx, filter, bson.M{"$set": job})
	return err
}

func (r *JobRepository) GetJobs(ctx context.Context) ([]Job, error) {
	var jobs []Job
	cursor, err := r.coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var job Job
		if err := cursor.Decode(&job); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (r *JobRepository) DeleteJob(context.Context,jobs)[] error {
	_,err := r.coll.DeleteAny(ctx,jobs)

	if err != nil {
		return err
	}
	return nil
}

func (r *JobRepository) DeleteJobs(ctx context.Context, jobs)[Job] error {
	_, err := r.coll.DeleteMany(ctx,jobs)
	return err
}
