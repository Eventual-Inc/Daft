package datarepo

import "path/filepath"

// Config for storage of datarepos
type DatarepoConfig interface {
	// Gets a fully qualified path to the underlying storage for this datarepo
	// For example, an S3 Datarepo would return s3://<bucket>/<path>/
	GetPath(name string, version string) string
	GetPartfilePath(name string, version string, partId string) string
}

type S3DatarepoConfig struct {
	S3bucket string
	S3prefix string
}

func (config *S3DatarepoConfig) GetPath(name string, version string) string {
	return "s3://" + filepath.Join(config.S3bucket, config.S3prefix, name, version)
}

func (config *S3DatarepoConfig) GetPartfilePath(name string, version string, partId string) string {
	return "s3://" + filepath.Join(config.S3bucket, config.S3prefix, name, version, partId)
}
