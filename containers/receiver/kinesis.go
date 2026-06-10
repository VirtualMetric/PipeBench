package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kintypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// receiveKinesis reads every shard of a Kinesis stream (LocalStack in the
// bench topology) from TRIM_HORIZON and counts each record. No deletion —
// Kinesis is a cursor-based log, so shard iterators do the draining. Shards
// are listed once at startup: bench streams are created by aws-init with a
// fixed shard count and never reshard mid-run.
func receiveKinesis(cfg config, cnt *counters, val *validator) error {
	stream := os.Getenv("RECEIVER_KINESIS_STREAM")
	if stream == "" {
		return fmt.Errorf("RECEIVER_KINESIS_STREAM is required for kinesis mode")
	}
	endpoint := getEnv("RECEIVER_KINESIS_ENDPOINT", getEnv("AWS_ENDPOINT_URL", "http://localstack:4566"))

	ctx := context.Background()
	awscfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}
	client := kinesis.NewFromConfig(awscfg, func(o *kinesis.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	shards, err := client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName: aws.String(stream),
	})
	if err != nil {
		return fmt.Errorf("list shards: %w", err)
	}
	if len(shards.Shards) == 0 {
		return fmt.Errorf("stream %q has no shards", stream)
	}
	fmt.Fprintf(os.Stderr, "receiver: polling kinesis stream=%q shards=%d\n", stream, len(shards.Shards))

	interval := cloudPollInterval()
	readShard := func(shard kintypes.Shard) error {
		itOut, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        aws.String(stream),
			ShardId:           shard.ShardId,
			ShardIteratorType: kintypes.ShardIteratorTypeTrimHorizon,
		})
		if err != nil {
			return fmt.Errorf("shard iterator %s: %w", aws.ToString(shard.ShardId), err)
		}
		it := itOut.ShardIterator
		// One shard (counter) per Kinesis shard goroutine.
		onLine := newCloudOnLine(cnt, val, cfg)
		for {
			out, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
				ShardIterator: it,
				Limit:         aws.Int32(10000),
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "receiver: kinesis get records: %v\n", err)
				time.Sleep(interval)
				continue
			}
			for _, rec := range out.Records {
				if err := countBody(rec.Data, onLine); err != nil {
					fmt.Fprintf(os.Stderr, "receiver: kinesis decode: %v\n", err)
				}
			}
			it = out.NextShardIterator
			if it == nil {
				return nil // shard closed
			}
			if len(out.Records) == 0 {
				time.Sleep(interval)
			}
		}
	}

	for _, shard := range shards.Shards[1:] {
		go func(s kintypes.Shard) {
			if err := readShard(s); err != nil {
				fmt.Fprintf(os.Stderr, "receiver: kinesis shard: %v\n", err)
			}
		}(shard)
	}
	return readShard(shards.Shards[0])
}
