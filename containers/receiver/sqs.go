package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// receiveSQS drains an SQS queue (LocalStack in the bench topology): receive
// → count each message body → delete. SNS-target cases are observed through
// this same mode via an SNS→SQS subscription with RawMessageDelivery=true,
// so message bodies are the original records. SQS caps each receive at 10
// messages, so RECEIVER_SQS_WORKERS parallel pollers (default 4) keep up
// with high-EPS subjects.
func receiveSQS(cfg config, cnt *counters, val *validator) error {
	queue := os.Getenv("RECEIVER_SQS_QUEUE")
	if queue == "" {
		return fmt.Errorf("RECEIVER_SQS_QUEUE is required for sqs mode")
	}
	endpoint := getEnv("RECEIVER_SQS_ENDPOINT", getEnv("AWS_ENDPOINT_URL", "http://localstack:4566"))
	workers := max(getEnvInt("RECEIVER_SQS_WORKERS", 4), 1)

	ctx := context.Background()
	awscfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}
	client := sqs.NewFromConfig(awscfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	urlOut, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(queue)})
	if err != nil {
		return fmt.Errorf("resolve queue %q: %w", queue, err)
	}
	queueURL := aws.ToString(urlOut.QueueUrl)
	fmt.Fprintf(os.Stderr, "receiver: polling sqs queue=%s workers=%d\n", queueURL, workers)

	poll := func() error {
		// One shard per worker so concurrent pollers don't contend.
		onLine := newCloudOnLine(cnt, val, cfg)
		return pollLoop("sqs", 0, func(ctx context.Context) error {
			out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueURL),
				MaxNumberOfMessages: 10,
				WaitTimeSeconds:     2, // long-poll: idle workers cost ~0 CPU
			})
			if err != nil {
				return fmt.Errorf("receive: %w", err)
			}
			if len(out.Messages) == 0 {
				return nil
			}
			entries := make([]sqstypes.DeleteMessageBatchRequestEntry, 0, len(out.Messages))
			for i, msg := range out.Messages {
				if err := countBody([]byte(aws.ToString(msg.Body)), onLine); err != nil {
					return fmt.Errorf("decode message: %w", err)
				}
				entries = append(entries, sqstypes.DeleteMessageBatchRequestEntry{
					Id:            aws.String(fmt.Sprintf("m%d", i)),
					ReceiptHandle: msg.ReceiptHandle,
				})
			}
			if _, err := client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
				QueueUrl: aws.String(queueURL),
				Entries:  entries,
			}); err != nil {
				return fmt.Errorf("delete batch: %w", err)
			}
			return nil
		})
	}

	for range workers - 1 {
		go func() {
			if err := poll(); err != nil {
				fmt.Fprintf(os.Stderr, "receiver: sqs worker: %v\n", err)
			}
		}()
	}
	return poll()
}
