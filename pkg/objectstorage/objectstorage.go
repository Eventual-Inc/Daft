package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ObjectStore is the interface for reading and writing to an external object storage service such as AWS S3
type ObjectStore interface {
	DownloadObject(ctx context.Context, path string, dst io.Writer) (n int64, err error)
}

// AWS S3 Implementation of ObjectStore
//
// Could be further optimized with https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager for large files,
// concurrent downloads, or more optimized copying behavior.

func splitS3Path(path string) (bucket string, key string, err error) {
	if !strings.HasPrefix(path, "s3://") {
		return "", "", errors.New(fmt.Sprintf("Path does not contain s3:// protocol prefix: %s", path))
	}
	bucket, key, found := strings.Cut(path[5:], "/")
	if !found {
		return "", "", errors.New(fmt.Sprintf("Error occurred when retrieving bucket and key from: %s", path))
	}
	return bucket, key, nil
}

type awsS3ObjectStore struct {
	s3Client *s3.Client
}

func NewAwsS3ObjectStore(ctx context.Context, cfg aws.Config) ObjectStore {
	return &awsS3ObjectStore{s3Client: s3.NewFromConfig(cfg)}
}

func (store *awsS3ObjectStore) DownloadObject(ctx context.Context, path string, dst io.Writer) (n int64, err error) {
	s3Bucket, s3Key, err := splitS3Path(path)
	if err != nil {
		return 0, err
	}
	getObjectOutput, err := store.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s3Bucket,
		Key:    &s3Key,
	})
	if err != nil {
		return 0, err
	}
	return io.Copy(dst, getObjectOutput.Body)
}
