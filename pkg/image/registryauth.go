package image

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
)

// RegistryAuthenticator is the interface for authenticating with a private Docker registry
type RegistryAuthenticator interface {
	GetUserAndSecret(ctx context.Context) (user string, secret string, err error)
}

// AWS ECR Implementation of RegistryAuth

type awsECRRegistryAuthenticator struct {
	ecrClient *ecr.Client
}

func NewECRRegistryAuthenticator(ctx context.Context, cfg aws.Config) RegistryAuthenticator {
	return &awsECRRegistryAuthenticator{ecrClient: ecr.NewFromConfig(cfg)}
}

func (auth *awsECRRegistryAuthenticator) GetUserAndSecret(ctx context.Context) (user string, secret string, err error) {
	authTokenOutput, err := auth.ecrClient.GetAuthorizationToken(ctx, &ecr.GetAuthorizationTokenInput{})
	if numTokens := len(*&authTokenOutput.AuthorizationData); numTokens != 1 {
		return "", "", errors.New(fmt.Sprintf("Expected 1 token returned from ECR, received: %d", numTokens))
	}
	token := *authTokenOutput.AuthorizationData[0].AuthorizationToken
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", "", fmt.Errorf("Unable to decode authorization token string as base64: %w", err)
	}

	decodedToken := string(decoded)
	i := strings.IndexByte(decodedToken, ':')
	if i <= 0 {
		return "", "", errors.New("Unable to find `:` character in supplied token")
	}

	secret = decodedToken[i+1:]
	user = decodedToken[0:i]

	return user, secret, nil

}
