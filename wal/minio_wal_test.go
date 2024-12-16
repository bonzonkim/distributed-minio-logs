package wal

import (
	"context"
	"crypto/rand"
	"distributed-minio-logs/utils"
	"encoding/hex"
	"fmt"
	"log"
	"testing"

	"github.com/minio/minio-go/v7"
)


func generateRandomStr() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func emptyOutBucket(ctx context.Context, client *minio.Client, bucketname, prefix string) error {
	objectCh := client.ListObjects(ctx, bucketname, minio.ListObjectsOptions{
		Prefix:		prefix,
		Recursive:	true,
	})
	
	var objectsToDelete []minio.ObjectInfo
	for object := range objectCh {
		if object.Err != nil {
			return fmt.Errorf("failed to list objects: %w", object.Err)
		}
		objectsToDelete = append(objectsToDelete, object)
	}

	if len(objectsToDelete) == 0 {
		log.Printf("No objects found in bucket %s ", bucketname)
		return nil
	}

	for _, object := range objectsToDelete {
		err := client.RemoveObject(ctx, bucketname, object.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to delete object %s: %v", object.Key, err)
		}
		log.Printf("Deleted object %s: ", object.Key)
	}
	fmt.Println("successfully empty out the bucket %s ", bucketname)
	return nil
}


func getWAL(t *testing.T) (*S3WAL, func()) {
	client := utils.CreateMinioClient()
	bucketName := "test-wal-bucket-" + generateRandomStr()
	prefix := generateRandomStr()

	if err := utils.CreateBucket(bucketName, client, context.Background(), "kr-standard", false); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		if err := emptyOutBucket(context.Background(), client, bucketName, prefix); err != nil {
			t.Logf("failed to empty out the bucket during cleanup: %v", err)
		}
		if err := client.RemoveBucket(context.Background(), bucketName); err != nil {
			t.Logf("failed to delete bucket during cleanup: %v", err)
		}
	}
	return NewS3WAL(client, bucketName, prefix), cleanup
}
