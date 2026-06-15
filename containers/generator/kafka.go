package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// runKafka produces records to a Kafka topic on the broker(s) at cfg.Target
// (comma-separated host:port seed brokers, e.g. "redpanda:9092").
//
// Records are packed cfg.KafkaBatch per Kafka message:
//   - KafkaBatch == 1: one JSON object is the message value verbatim.
//   - KafkaBatch  > 1: N JSON objects are packed as a JSON array
//     ("[obj1,obj2,...]") in a single message value.
//
// "lines" counts records (not Kafka messages), exactly like the OTLP mode
// counts LogRecords — so the harness's lines-sent vs lines-received comparison
// stays meaningful: a subject that splits the array re-emits one record per
// object. Each of the cfg.Connections producer workers runs its OWN franz-go
// client; counts/bytes are accumulated from the produce acknowledgements so a
// record only counts once Kafka accepts it.
func runKafka(cfg config, clock *sendClock) (int64, int64, error) {
	conns := cfg.Connections
	if conns < 1 {
		conns = 1
	}

	var ackedLines, ackedBytes atomic.Int64
	var firstErr error
	var errOnce sync.Once
	setErr := func(e error) { errOnce.Do(func() { firstErr = e }) }

	// One franz-go client per connection. A single shared client funnels every
	// worker's records through one buffer and one set of broker connections;
	// when the topic has multiple partitions the records batch thinner and
	// produce throughput sags (measured: ~1.6M rec/s across 4 partitions vs
	// ~2.1M to a single partition). Independent clients give each worker its own
	// buffer and in-flight pipeline, so produce scales with connections — and
	// the subject's consumer, not the generator, stays the bottleneck.
	runWorker := func(connID int) {
		client, err := newKafkaClient(cfg)
		if err != nil {
			setErr(err)
			return
		}
		defer client.Close()

		// produce enqueues one Kafka message carrying `records` JSON objects.
		// The promise fires on ack; the value slice is owned by the client until
		// then, so callers must hand it a fresh copy (see kafkaWorker).
		produce := func(value []byte, records int64) {
			client.Produce(context.Background(), &kgo.Record{Topic: cfg.KafkaTopic, Value: value},
				func(_ *kgo.Record, perr error) {
					if perr != nil {
						setErr(perr)
						return
					}
					ackedLines.Add(records)
					ackedBytes.Add(int64(len(value)))
				})
			clock.RecordSend()
		}

		kafkaWorker(cfg, connID, produce)

		// Block until every buffered record is acked (or errored) so the counts
		// reflect what Kafka actually accepted.
		if err := client.Flush(context.Background()); err != nil {
			setErr(err)
		}
	}

	if conns == 1 {
		runWorker(0)
	} else {
		var wg sync.WaitGroup
		for i := 0; i < conns; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				runWorker(id)
			}(i + cfg.ConnOffset)
		}
		wg.Wait()
	}

	return ackedLines.Load(), ackedBytes.Load(), firstErr
}

// kafkaWorker drives the shared send loop for one producer connection,
// accumulating records into KafkaBatch-sized messages and handing each
// finished message value to produce(). It reuses sendLinesConn so rate
// limiting, total_lines/duration bounds, sample_file replay and format=json
// synthesis behave identically to the other generator modes.
func kafkaWorker(cfg config, connID int, produce func(value []byte, records int64)) {
	batchN := cfg.KafkaBatch
	if batchN < 1 {
		batchN = 1
	}
	array := batchN > 1

	var buf bytes.Buffer
	var inBatch int64

	flush := func() {
		if inBatch == 0 {
			return
		}
		if array {
			buf.WriteByte(']')
		}
		// The client holds the value until ack, and we reset buf below, so
		// hand produce its own copy.
		value := make([]byte, buf.Len())
		copy(value, buf.Bytes())
		produce(value, inBatch)
		buf.Reset()
		inBatch = 0
	}

	// sendLinesConn calls write(line) once per record; `line` carries a
	// trailing '\n' and may alias a reused buffer (templateLine/seqBuf), so we
	// copy the newline-trimmed record straight into the message buffer here.
	_, _, err := sendLinesConn(cfg, connID, &sendClock{}, func(line []byte) error {
		rec := bytes.TrimRight(line, "\r\n")
		if inBatch == 0 && array {
			buf.WriteByte('[')
		} else if inBatch > 0 {
			buf.WriteByte(',')
		}
		buf.Write(rec)
		inBatch++
		if inBatch >= int64(batchN) {
			flush()
		}
		return nil
	})
	flush() // emit the final partial batch
	if err != nil {
		fmt.Fprintf(os.Stderr, "generator: kafka worker %d send error: %v\n", connID, err)
	}
}

// newKafkaClient builds a franz-go producer for the seed brokers in cfg.Target.
// The default idempotent producer (acks=all) is kept so producer-side delivery
// is exactly-once into the topic — loss/duplication then measures the subject,
// not the generator. A short linger lets the client coalesce records from the
// concurrent workers into fewer produce requests.
func newKafkaClient(cfg config) (*kgo.Client, error) {
	brokers := strings.Split(cfg.Target, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(cfg.KafkaTopic),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerLinger(5 * time.Millisecond),
		// Deep buffer so a fast producer goroutine isn't throttled waiting on
		// acks — Produce only blocks once this many records are unacked.
		kgo.MaxBufferedRecords(500_000),
	}

	// SASL auth, when the broker requires it. PLAIN and SCRAM-SHA-256/512 are
	// in the franz-go module already vendored for the producer.
	switch cfg.KafkaSASL {
	case "", "none":
		// no SASL
	case "plain":
		opts = append(opts, kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{User: cfg.KafkaUser, Pass: cfg.KafkaPassword}, nil
		})))
	case "scram-sha-256":
		opts = append(opts, kgo.SASL(scram.Sha256(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: cfg.KafkaUser, Pass: cfg.KafkaPassword}, nil
		})))
	case "scram-sha-512":
		opts = append(opts, kgo.SASL(scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: cfg.KafkaUser, Pass: cfg.KafkaPassword}, nil
		})))
	default:
		return nil, fmt.Errorf("kafka: unknown SASL mechanism %q", cfg.KafkaSASL)
	}

	// TLS, when the broker listens over TLS. buildTLSConfig trusts /certs/ca.crt
	// and presents /certs/client.{crt,key} if present (mTLS) — the same cert
	// material the harness mounts at /certs.
	if cfg.KafkaTLS {
		tlsCfg, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("kafka tls config: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client for %q: %w", cfg.Target, err)
	}
	// Fail fast with a clear error if the broker is unreachable, rather than
	// blocking the whole run on buffered produces that never ack.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka: broker %q not reachable: %w", cfg.Target, err)
	}
	return client, nil
}
