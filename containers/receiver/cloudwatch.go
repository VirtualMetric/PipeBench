package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

// receiveCloudWatch polls every stream of a CloudWatch Logs group (LocalStack
// in the bench topology) and counts each event message. Streams are
// re-discovered every sweep — subjects create their own streams at arbitrary
// times. Per-stream forward tokens make each sweep incremental.
func receiveCloudWatch(cfg config, cnt *counters, val *validator) error {
	group := os.Getenv("RECEIVER_CWL_GROUP")
	if group == "" {
		return fmt.Errorf("RECEIVER_CWL_GROUP is required for cloudwatch mode")
	}
	endpoint := getEnv("RECEIVER_CWL_ENDPOINT", getEnv("AWS_ENDPOINT_URL", "http://localstack:4566"))

	awscfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}
	client := cloudwatchlogs.NewFromConfig(awscfg, func(o *cloudwatchlogs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	onLine := newCloudOnLine(cnt, val, cfg)
	// nextForwardToken per stream — GetLogEvents with the previous token
	// returns only new events, so each sweep is incremental.
	tokens := map[string]*string{}
	fmt.Fprintf(os.Stderr, "receiver: polling cloudwatch group=%q endpoint=%s\n", group, endpoint)

	return pollLoop("cloudwatch", cloudPollInterval(), func(ctx context.Context) error {
		var streamCursor *string
		for {
			streams, err := client.DescribeLogStreams(ctx, &cloudwatchlogs.DescribeLogStreamsInput{
				LogGroupName: aws.String(group),
				NextToken:    streamCursor,
			})
			if err != nil {
				return fmt.Errorf("describe streams: %w", err)
			}
			for _, stream := range streams.LogStreams {
				name := aws.ToString(stream.LogStreamName)
				token := tokens[name]
				for {
					out, err := client.GetLogEvents(ctx, &cloudwatchlogs.GetLogEventsInput{
						LogGroupName:  aws.String(group),
						LogStreamName: aws.String(name),
						StartFromHead: aws.Bool(true),
						NextToken:     token,
					})
					if err != nil {
						return fmt.Errorf("get events %s: %w", name, err)
					}
					for _, ev := range out.Events {
						onLine([]byte(aws.ToString(ev.Message)))
					}
					// The token stops advancing when the stream is drained.
					if aws.ToString(out.NextForwardToken) == aws.ToString(token) {
						break
					}
					token = out.NextForwardToken
				}
				tokens[name] = token
			}
			if streams.NextToken == nil {
				return nil
			}
			streamCursor = streams.NextToken
		}
	})
}
