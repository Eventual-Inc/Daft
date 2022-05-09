package datarepo

import (
	"context"
	"fmt"

	"github.com/Eventual-Inc/Daft/pkg/objectstorage"
	"github.com/aws/aws-sdk-go-v2/config"
)

func ObjectStoreFactory(locationConfig ManifestConfig) (objectstorage.ObjectStore, error) {
	switch locationConfig.Kind() {
	case DatasourceIDAWSS3:
		ctx := context.TODO()
		awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-west-2"))
		if err != nil {
			return nil, err
		}
		return objectstorage.NewAwsS3ObjectStore(ctx, awsConfig), nil
	default:
		return nil, fmt.Errorf("object store for %s not implemented", locationConfig.Kind())
	}
}
