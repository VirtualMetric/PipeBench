package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
)

// receiveAzureBlob polls an Azure Blob container (Azurite in the bench
// topology), counts every line of every blob, and deletes drained blobs —
// same destructive-drain contract as the s3 mode.
func receiveAzureBlob(cfg config, cnt *counters, val *validator) error {
	connString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	if connString == "" {
		return fmt.Errorf("AZURE_STORAGE_CONNECTION_STRING is required for azure_blob mode")
	}
	container := os.Getenv("RECEIVER_AZURE_CONTAINER")
	if container == "" {
		return fmt.Errorf("RECEIVER_AZURE_CONTAINER is required for azure_blob mode")
	}
	prefix := os.Getenv("RECEIVER_AZURE_PREFIX")

	client, err := azblob.NewClientFromConnectionString(connString, nil)
	if err != nil {
		return fmt.Errorf("azure client: %w", err)
	}

	onLine := newCloudOnLine(cnt, val, cfg)
	fmt.Fprintf(os.Stderr, "receiver: polling azure blob container=%q prefix=%q\n", container, prefix)

	return pollLoop("azure_blob", cloudPollInterval(), func(ctx context.Context) error {
		pager := client.NewListBlobsFlatPager(container, &azblob.ListBlobsFlatOptions{
			Prefix: &prefix,
		})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("list blobs: %w", err)
			}
			for _, item := range page.Segment.BlobItems {
				name := *item.Name
				resp, err := client.DownloadStream(ctx, container, name, nil)
				if err != nil {
					return fmt.Errorf("download blob %s: %w", name, err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					return fmt.Errorf("read blob %s: %w", name, err)
				}
				if err := countBody(body, onLine); err != nil {
					return fmt.Errorf("decode blob %s: %w", name, err)
				}
				if _, err := client.DeleteBlob(ctx, container, name, nil); err != nil {
					return fmt.Errorf("delete blob %s: %w", name, err)
				}
			}
		}
		return nil
	})
}

// runAzureInit is the one-shot `azure-init` compose service: it retries blob
// container + same-named storage queue creation until Azurite answers, then
// exits 0 so services gated on service_completed_successfully start. The
// queue carries the synthetic BlobCreated events vmetric's azblob listener
// polls for (the generator enqueues them; Azurite never emits them itself).
// Reusing the receiver image here avoids pulling a dedicated azure-cli image.
func runAzureInit() {
	connString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	containersEnv := os.Getenv("AZURE_INIT_CONTAINERS")
	if connString == "" || containersEnv == "" {
		fmt.Fprintln(os.Stderr, "azure-init: AZURE_STORAGE_CONNECTION_STRING and AZURE_INIT_CONTAINERS are required")
		os.Exit(1)
	}

	blobClient, err := azblob.NewClientFromConnectionString(connString, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "azure-init: blob client: %v\n", err)
		os.Exit(1)
	}
	queueClient, err := azqueue.NewServiceClientFromConnectionString(connString, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "azure-init: queue client: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Minute)
	for name := range strings.SplitSeq(containersEnv, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		for {
			_, err := blobClient.CreateContainer(ctx, name, nil)
			if err == nil || bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
				break
			}
			if time.Now().After(deadline) {
				fmt.Fprintf(os.Stderr, "azure-init: create container %s: %v\n", name, err)
				os.Exit(1)
			}
			time.Sleep(time.Second)
		}
		for {
			_, err := queueClient.CreateQueue(ctx, name, nil)
			if err == nil || strings.Contains(err.Error(), "QueueAlreadyExists") {
				break
			}
			if time.Now().After(deadline) {
				fmt.Fprintf(os.Stderr, "azure-init: create queue %s: %v\n", name, err)
				os.Exit(1)
			}
			time.Sleep(time.Second)
		}
		fmt.Fprintf(os.Stderr, "azure-init: container+queue %q ready\n", name)
	}
	os.Exit(0)
}
