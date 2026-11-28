package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// receiveKafka is RECEIVER_MODE=kafka: it reads a topic that the subject
// writes to (RECEIVER_KAFKA_TOPIC) from its start, and hands each line of each
// record's value to the counters and the sequence tracker. A record that the
// topic holds twice counts its lines twice. It reads no consumer group, so it
// commits nothing, and a receiver that restarts reads the topic again from its
// start.
func receiveKafka(cfg config, cnt *counters, val *validator) error {
	brokers := strings.Split(getEnv("RECEIVER_KAFKA_BROKERS", "kafka:9092"), ",")
	topic := getEnv("RECEIVER_KAFKA_TOPIC", "events")
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// The subject may make the topic itself, after the receiver starts
		kgo.AllowAutoTopicCreation(),
		kgo.MetadataMinAge(time.Second),
	)
	if err != nil {
		return err
	}
	defer client.Close()
	fmt.Fprintf(os.Stderr, "receiver: kafka: reading topic %q from %v\n", topic, brokers)
	onLine := newCloudOnLine(cnt, val, cfg)
	for {
		fetches := client.PollFetches(context.Background())
		fetches.EachError(func(t string, p int32, err error) {
			fmt.Fprintf(os.Stderr, "receiver: kafka: %s/%d: %v\n", t, p, err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			for line := range bytes.SplitSeq(r.Value, []byte{'\n'}) {
				if len(bytes.TrimSpace(line)) > 0 {
					onLine(line)
				}
			}
		})
	}
}
