package utils

import (
	"distributed-minio-logs/env"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)


func CreateMinioClient() *minio.Client {
	keys := env.LoadKeys()

	client, err := minio.New(keys.Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(keys.AccessKeyID, keys.SecretAccessKey, ""),
		Secure: true,
	})
	if err != nil {
		log.Fatalf("failed to connect to storage: %v", err)
	}

	return client
}
