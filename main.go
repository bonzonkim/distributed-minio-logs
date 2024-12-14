package main

import (
	"context"
	"distributed-minio-logs/utils"
	"fmt"
	"log"
)


func main() {
	client := utils.CreateMinioClient()
	ctx := context.Background()

	if err := utils.CreateBucket("wal", client, ctx, "kr-standard", false); err != nil {
		log.Fatalf("failed to create bucket: %v", err)
	}

	buckets, err := client.ListBuckets(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	for _, bucket := range buckets {
		fmt.Println(bucket.Name)
	}
}
