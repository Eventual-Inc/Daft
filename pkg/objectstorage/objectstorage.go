package objectstorage

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type DownloadObjectOption = func(*s3.GetObjectInput)

func WithDownloadRange(start int, end int) DownloadObjectOption {
	f := func(in *s3.GetObjectInput) {
		r := fmt.Sprintf("bytes=%d-%d", start, end)
		in.Range = &r
	}
	return f
}

// ObjectStore is the interface for reading and writing to an external object storage service such as AWS S3
type ObjectStore interface {
	DownloadObject(ctx context.Context, path string, opts ...DownloadObjectOption) (io.Reader, error)
	ListObjects(ctx context.Context, path string) ([]string, error)
}

// AWS S3 Implementation of ObjectStore
//
// Could be further optimized with https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager for large files,
// concurrent downloads, or more optimized copying behavior.

func splitS3Path(path string) (bucket string, key string, err error) {
	if !strings.HasPrefix(path, "s3://") {
		return "", "", fmt.Errorf("path does not contain s3:// protocol prefix: %s", path)
	}
	bucket, key, found := strings.Cut(path[5:], "/")
	if !found {
		return "", "", fmt.Errorf("error occurred when retrieving bucket and key from: %s", path)
	}
	return bucket, key, nil
}

type awsS3ObjectStore struct {
	s3Client *s3.Client
}

func NewAwsS3ObjectStore(ctx context.Context, cfg aws.Config) ObjectStore {
	return &awsS3ObjectStore{s3Client: s3.NewFromConfig(cfg)}
}

func (store *awsS3ObjectStore) DownloadObject(ctx context.Context, path string, opts ...DownloadObjectOption) (io.Reader, error) {
	s3Bucket, s3Key, err := splitS3Path(path)
	if err != nil {
		return nil, err
	}
	in := s3.GetObjectInput{
		Bucket: &s3Bucket,
		Key:    &s3Key,
	}
	for _, opt := range opts {
		opt(&in)
	}
	getObjectOutput, err := store.s3Client.GetObject(ctx, &in)
	if err != nil {
		return nil, err
	}
	return getObjectOutput.Body, nil
}

func (store *awsS3ObjectStore) ListObjects(ctx context.Context, path string) ([]string, error) {
	s3Bucket, s3Key, err := splitS3Path(path)
	if err != nil {
		return nil, err
	}
	listObjectOutput, err := store.s3Client.ListObjectsV2(
		ctx,
		// TODO(jchia): Need to handle pagination here with LastKey, otherwise this
		// defaults to returning a maximum of 1,000 keys only
		&s3.ListObjectsV2Input{
			Bucket: &s3Bucket,
			Prefix: &s3Key,
		},
	)
	if err != nil {
		return nil, err
	}
	objectPaths := []string{}
	for _, objMetadata := range listObjectOutput.Contents {
		objectPaths = append(objectPaths, fmt.Sprintf("s3://%s/%s", s3Bucket, *objMetadata.Key))
	}
	return objectPaths, nil
}
