package utils

import (
	"context"
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

func CreateBucket(bucketName string, client *minio.Client, ctx context.Context, region string, bucketBlock bool) error {
	if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{
		Region:        region,
		ObjectLocking: bucketBlock,
	}); err != nil {
		log.Fatalf("failed to make bucket %v", err)
		return err
	}
	return nil
}


//-------------------------------------
//import (
//	"distributed-minio-logs/env"
//
//	"github.com/aws/aws-sdk-go-v2/aws"
//	"github.com/aws/aws-sdk-go-v2/credentials"
//	"github.com/aws/aws-sdk-go-v2/service/s3"
//)
//func SetupMinioClient() *s3.Client {
//	// https://stackoverflow.com/a/78815403
//	// thank you lurenyang
//	keys := env.LoadKeys()
//	return s3.NewFromConfig(aws.Config{Region: "kr-standard"}, func(o *s3.Options) {
//		o.BaseEndpoint = aws.String(keys.Endpoint)
//		o.Credentials = credentials.NewStaticCredentialsProvider(keys.AccessKeyID, keys.SecretAccessKey, "")
//	})
//}
