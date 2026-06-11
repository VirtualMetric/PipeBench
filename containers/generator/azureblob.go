package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
)

// runAzureBlob uploads synthesized lines as block blobs (Azurite in the
// bench topology) for cases where the subject ingests FROM blob storage.
// cfg.Target is the Azurite endpoint (readiness dial only); the client is
// built from AZURE_STORAGE_CONNECTION_STRING.
//
// With GENERATOR_AZURE_QUEUE_EVENTS=true, every upload also enqueues a
// base64 EventGrid BlobCreated message to the storage queue named after the
// container — Azurite never emits these itself, and vmetric's azblob
// listener consumes blobs exclusively through that queue. Subjects that list
// containers directly leave it off.
func runAzureBlob(cfg config, clock *sendClock) (int64, int64, error) {
	connString := mustEnv("AZURE_STORAGE_CONNECTION_STRING")
	container := mustEnv("GENERATOR_AZURE_CONTAINER")
	prefix := getEnv("GENERATOR_AZURE_PREFIX", "in/")
	linesPerBlob := max(getEnvInt("GENERATOR_AZURE_LINES_PER_BLOB", 10000), 1)
	queueEvents := getEnvBool("GENERATOR_AZURE_QUEUE_EVENTS", false)

	blobClient, err := azblob.NewClientFromConnectionString(connString, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("azure blob client: %w", err)
	}
	var queueClient *azqueue.QueueClient
	if queueEvents {
		svc, err := azqueue.NewServiceClientFromConnectionString(connString, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("azure queue client: %w", err)
		}
		queueClient = svc.NewQueueClient(container)
	}

	upload := func(ctx context.Context, connID int, seq int64, data []byte) error {
		name := fmt.Sprintf("%s%d/%06d-%d.log", prefix, connID, seq, time.Now().UnixNano())
		if _, err := blobClient.UploadBuffer(ctx, container, name, data, nil); err != nil {
			return fmt.Errorf("upload blob %s: %w", name, err)
		}
		if queueClient != nil {
			msg, err := blobCreatedEvent(container, name, len(data))
			if err != nil {
				return fmt.Errorf("event for blob %s: %w", name, err)
			}
			if _, err := queueClient.EnqueueMessage(ctx, msg, nil); err != nil {
				return fmt.Errorf("enqueue event for blob %s: %w", name, err)
			}
		}
		clock.RecordSend()
		return nil
	}

	return runObjectWorkers(cfg, func(connID int) (int64, int64, error) {
		return packLines(cfg, connID, clock, linesPerBlob, func(seq int64, object []byte) error {
			return upload(context.Background(), connID, seq, object)
		})
	})
}

// blobCreatedEvent builds the base64-encoded EventGrid BlobCreated JSON that
// vmetric's azblob listener dequeues: it requires eventType
// "Microsoft.Storage.BlobCreated", a non-empty eventTime, and a subject of
// the form /blobServices/default/containers/<container>/blobs/<name>.
func blobCreatedEvent(container, name string, size int) (string, error) {
	now := time.Now().UTC()
	event := map[string]any{
		"id":              fmt.Sprintf("bench-%d", now.UnixNano()),
		"topic":           "/subscriptions/bench/resourceGroups/bench/providers/Microsoft.Storage/storageAccounts/devstoreaccount1",
		"subject":         fmt.Sprintf("/blobServices/default/containers/%s/blobs/%s", container, name),
		"eventType":       "Microsoft.Storage.BlobCreated",
		"eventTime":       now.Format(time.RFC3339Nano),
		"dataVersion":     "",
		"metadataVersion": "1",
		"data": map[string]any{
			"api":           "PutBlob",
			"blobType":      "BlockBlob",
			"contentLength": size,
			"url":           fmt.Sprintf("http://azurite:10000/devstoreaccount1/%s/%s", container, name),
		},
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}
