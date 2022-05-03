package image

import (
	"context"
	"strings"
	"errors"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	dockerconfig "github.com/containerd/containerd/remotes/docker/config"
)

func ResolverFactory(ctx context.Context, uri string) (remotes.Resolver, error) {
	if strings.Contains(uri, ".ecr.") {
		return buildECRResolver(ctx)
	} else {
		return nil, errors.New("No resolver found for: " + uri)
	}
}


func buildECRResolver(ctx context.Context) (remotes.Resolver, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
	authenticator := NewECRRegistryAuthenticator(context.TODO(), cfg)
	user, secret, err := authenticator.GetUserAndSecret(context.TODO())
	if err != nil {
		return nil, err
	}

	// Get a Resolver using the username and secret
	options := docker.ResolverOptions{
		Tracker: docker.NewInMemoryTracker(),
	}
	hostOptions := dockerconfig.HostOptions{}
	hostOptions.Credentials = func(host string) (string, string, error) {
		// If host doesn't match...
		// Only one host
		return user, secret, nil
	}
	options.Hosts = dockerconfig.ConfigureHosts(context.TODO(), hostOptions)
	resolver := docker.NewResolver(options)
	return resolver, nil;
} 