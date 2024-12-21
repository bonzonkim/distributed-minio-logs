package wal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"distributed-minio-logs/utils"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/minio/minio-go/v7"
)

type S3WAL struct {
	client		*minio.Client
	bucketName	string
	prefix	    string
	length	    uint64
}

func NewS3WAL(client *minio.Client, bucketName string, prefix string) *S3WAL {
	return &S3WAL{
		client:		client,
		bucketName: bucketName, 
		prefix:		prefix,
		length:		0,
	}
}

func (w *S3WAL) getObjectKey(offset uint64) string {
	return w.prefix + "/" + fmt.Sprintf("%020d", offset)
}

func (w *S3WAL) getOffsetFromKey(key string) (uint64, error) {
	// if prefix is 'logs', 'logs/00000000000000000001'
	// len(w.prefix) is 4, then plus 1 => 5
	// : means slice from 5 to end
	// it'll return 00000000000000000001
	numStr := key[len(w.prefix)+1:] 
	return strconv.ParseUint(numStr, 10, 64)
}

func calculateCheckSum(buf *bytes.Buffer) [32]byte {
	return sha256.Sum256(buf.Bytes())
}

func validateChecksum(data []byte) bool {
	var storedChecksum [32]byte
	copy(storedChecksum[:], data[len(data)-32:])
	recordData := data[:len(data)-32]
	return storedChecksum == calculateCheckSum(bytes.NewBuffer(recordData))
}

func prepareBody(offset uint64, data []byte) ([]byte, error) {
	// 8 bytes for offset, len(data) bytes for data, 32 bytes for checksum
	bufferLen := 8 + len(data) + 32
	buf := bytes.NewBuffer(make([]byte, 0, bufferLen))
	if err := binary.Write(buf, binary.BigEndian, offset); err != nil {
		return nil, err
	}
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}
	checksum := calculateCheckSum(buf)
	_, err := buf.Write(checksum[:])
	return buf.Bytes(), err
}

func (w *S3WAL) Append(ctx context.Context, data []byte) (uint64, error) {
	nextOffset := w.length + 1

	buf, err := prepareBody(nextOffset, data)
	if err != nil {
		return 0, fmt.Errorf("Failed to prepare object body: %w", err)
	}

	if _, err = w.client.PutObject(ctx, w.bucketName, w.getObjectKey(nextOffset), bytes.NewReader(buf), int64(w.length), minio.PutObjectOptions{
		ContentType: "application/octet-stream", // Set a suitable content type
	}); err != nil {
		return 0, fmt.Errorf("failed to put object to storage: %w", err)
	}

	w.length = nextOffset
	return nextOffset, nil
}

func (w *S3WAL) Read(ctx context.Context, offset uint64) (Record, error) {
	key := w.getObjectKey(offset)

	//(ctx context.Context, bucketName string, objectName string, opts minio.GetObjectOptions)
	result, err := w.client.GetObject(ctx, w.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return Record{}, fmt.Errorf("Failed to get object from storage: %w", err)
	}
	//defer result.Body.Close()
	defer result.Close()

	data, err := io.ReadAll(result)
	if err != nil {
		return Record{}, fmt.Errorf("failed to read object body: %w", err)
	}
	if len(data) < 40 {
		return Record{}, fmt.Errorf("Invalid record: data too short")
	}

	var storedOffset uint64
	if err = binary.Read(bytes.NewReader(data[:8]), binary.BigEndian, &storedOffset); err != nil {
		return Record{}, err
	}
	if storedOffset != offset {
		return Record{}, fmt.Errorf("offset mismatch: expected %d, got %d", offset, storedOffset)
	}
	if !validateChecksum(data) {
		return Record{}, fmt.Errorf("checksum mismatch")
	}
	return Record{
		Offset: storedOffset, 
		Data:	data[8 : len(data)-32],
	}, nil
}

func (w *S3WAL) LastRecord(ctx context.Context) (Record, error) {
	client := utils.CreateMinioClient()
	objectCh := client.ListObjects(ctx, w.bucketName, minio.ListObjectsOptions{
		Prefix:		w.prefix + "/",
		Recursive:	true,
	})

	var maxOffset uint64 = 0
	for object := range objectCh {
		if object.Err != nil {
			return Record{}, fmt.Errorf("failed to list object: %w", object.Err)
		}
		key := object.Key
		offset, err := w.getOffsetFromKey(key)
		if err != nil {
			return Record{}, fmt.Errorf("failed to parse offset from key: %w", err)
		}
		if offset > maxOffset {
			maxOffset = offset
		}
	}
	if maxOffset == 0 {
		return Record{}, fmt.Errorf("WAL is empty")
	}
	w.length = maxOffset
	return w.Read(ctx, maxOffset)
}
