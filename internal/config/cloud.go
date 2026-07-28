package config

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Emulator constants. These are deliberately public, documented values —
// LocalStack accepts any static credential pair and Azurite ships a
// well-known development-storage account. They are never real credentials;
// the harness never reads cloud credentials from the host environment.
const (
	// AWSEmulatorAccessKey / AWSEmulatorSecretKey are the dummy static
	// credentials every container uses against LocalStack.
	AWSEmulatorAccessKey = "test"
	AWSEmulatorSecretKey = "test"
	// AWSEmulatorAccountID is LocalStack's fixed account id, used to build
	// queue URLs and ARNs.
	AWSEmulatorAccountID = "000000000000"

	// AzuriteAccount / AzuriteKey are Azurite's published development-storage
	// account name and key (the same pair every Azurite install accepts).
	AzuriteAccount = "devstoreaccount1"
	AzuriteKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

	// MinioRootUser / MinioRootPassword are the static credentials the
	// MinIO emulator boots with and every container uses against it. Like
	// the values above they are emulator-only and never real; the harness
	// never reads cloud credentials from the host environment. The password
	// is >=8 chars because MinIO refuses to start otherwise.
	MinioRootUser     = "pipebench"
	MinioRootPassword = "pipebench-minio-dev"

	// KafkaSASLUser / KafkaSASLPassword are the static SASL credentials the
	// harness bootstraps on the Redpanda broker for `kafka.auth` cases and
	// hands to the generator and subject. Emulator-only, never real; injected
	// into the subject as KAFKA_SASL_PASSWORD (the case configs read
	// ${KAFKA_SASL_PASSWORD}). The password is non-trivial because a plaintext
	// device password trips the director's credential decryptor — see
	// KAFKA_TESTS.md.
	KafkaSASLUser     = "bench"
	KafkaSASLPassword = "pipebench-kafka-dev"
)

// AWSConfig, when set on a TestCase (`aws:`), adds a LocalStack emulator to
// the test topology: the harness renders a `localstack` service whose
// init-hook script creates the resources declared below before the subject
// starts (the service healthcheck gates on init completion). Subject,
// generator and receiver containers all receive the emulator endpoint and
// dummy credentials via environment.
type AWSConfig struct {
	// Image is the LocalStack container image (default "localstack/localstack:4").
	Image string `yaml:"image"`
	// Region is the emulated region (default "us-east-1").
	Region string `yaml:"region"`
	// Services overrides the LocalStack SERVICES env. Default: derived from
	// the resources declared below.
	Services []string `yaml:"services"`

	// Buckets are S3 buckets created at init.
	Buckets []string `yaml:"buckets"`
	// Queues are SQS queues created at init.
	Queues []string `yaml:"queues"`
	// Topics are SNS topics created at init.
	Topics []string `yaml:"topics"`
	// Streams are Kinesis streams created at init.
	Streams []AWSStream `yaml:"streams"`
	// LogGroups are CloudWatch Logs groups (with optional stream) created at init.
	LogGroups []AWSLogGroup `yaml:"log_groups"`

	// BucketNotifications wires S3 → SQS event notifications
	// (s3:ObjectCreated:*). Required by SQS-driven S3 sources (vmetric
	// queue_url, vector aws_s3, filebeat aws-s3).
	BucketNotifications []AWSBucketNotification `yaml:"bucket_notifications"`
	// Subscriptions wires SNS → SQS (RawMessageDelivery=true) so an
	// SNS-target case is observable by the `sqs` receiver mode.
	Subscriptions []AWSSubscription `yaml:"subscriptions"`

	// SeedObjects pre-uploads synthetic objects into a declared bucket at
	// init, before the subject starts — for list-mode / first-run backlog
	// source cases where the subject must find objects already present rather
	// than a generator streaming them in during the run.
	SeedObjects []AWSSeedObjects `yaml:"seed_objects"`
}

// AWSSeedObjects pre-uploads Objects synthetic objects (Lines lines each,
// each line prefixed with Marker) under Prefix in Bucket during LocalStack
// init.
type AWSSeedObjects struct {
	// Bucket must be one of the declared Buckets.
	Bucket string `yaml:"bucket"`
	// Prefix is the key prefix for seeded objects (default "seed/").
	Prefix string `yaml:"prefix"`
	// Objects is the number of objects to create (> 0).
	Objects int `yaml:"objects"`
	// Lines is the number of lines per object (> 0).
	Lines int `yaml:"lines"`
	// Marker is the per-line content prefix (default "SEED"); validated
	// against the cloud-name charset so it cannot inject into the init shell
	// script.
	Marker string `yaml:"marker"`
}

// AWSStream declares a Kinesis stream created at init.
type AWSStream struct {
	Name string `yaml:"name"`
	// Shards is the stream shard count (default 1).
	Shards int `yaml:"shards"`
}

// AWSLogGroup declares a CloudWatch Logs group (and optional stream) created at init.
type AWSLogGroup struct {
	Group string `yaml:"group"`
	// Stream is an optional log stream pre-created inside Group. Subjects
	// that create their own streams can leave it empty.
	Stream string `yaml:"stream"`
}

// AWSBucketNotification wires s3:ObjectCreated:* events on Bucket to Queue.
type AWSBucketNotification struct {
	Bucket string `yaml:"bucket"`
	Queue  string `yaml:"queue"`
}

// AWSSubscription subscribes Queue to Topic with RawMessageDelivery=true.
type AWSSubscription struct {
	Topic string `yaml:"topic"`
	Queue string `yaml:"queue"`
}

// ImageOrDefault returns the LocalStack image, defaulting like KafkaConfig does.
func (a *AWSConfig) ImageOrDefault() string {
	if a != nil && a.Image != "" {
		return a.Image
	}
	return "localstack/localstack:4"
}

// RegionOrDefault returns the emulated AWS region.
func (a *AWSConfig) RegionOrDefault() string {
	if a != nil && a.Region != "" {
		return a.Region
	}
	return "us-east-1"
}

// ServicesOrDefault returns the comma-joined LocalStack SERVICES value —
// either the explicit override or the set derived from declared resources.
func (a *AWSConfig) ServicesOrDefault() string {
	if a == nil {
		return ""
	}
	if len(a.Services) > 0 {
		return strings.Join(a.Services, ",")
	}
	set := map[string]struct{}{}
	if len(a.Buckets) > 0 {
		set["s3"] = struct{}{}
	}
	if len(a.Queues) > 0 {
		set["sqs"] = struct{}{}
	}
	if len(a.Topics) > 0 {
		set["sns"] = struct{}{}
	}
	if len(a.Streams) > 0 {
		set["kinesis"] = struct{}{}
	}
	if len(a.LogGroups) > 0 {
		set["logs"] = struct{}{}
	}
	// STS answers the SDK identity checks subjects commonly run at startup.
	set["sts"] = struct{}{}
	services := make([]string, 0, len(set))
	for s := range set {
		services = append(services, s)
	}
	sort.Strings(services)
	return strings.Join(services, ",")
}

// EndpointURL is the LocalStack edge endpoint as seen from the bench network.
func (a *AWSConfig) EndpointURL() string { return "http://localstack:4566" }

// QueueURL builds the LocalStack URL for a declared queue, for use in
// subject configs and receiver env.
func (a *AWSConfig) QueueURL(queue string) string {
	return fmt.Sprintf("%s/%s/%s", a.EndpointURL(), AWSEmulatorAccountID, queue)
}

// QueueARN builds the emulator ARN for a declared queue.
func (a *AWSConfig) QueueARN(queue string) string {
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", a.RegionOrDefault(), AWSEmulatorAccountID, queue)
}

// TopicARN builds the emulator ARN for a declared topic.
func (a *AWSConfig) TopicARN(topic string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:%s", a.RegionOrDefault(), AWSEmulatorAccountID, topic)
}

// AzureConfig, when set on a TestCase (`azure:`), adds an Azurite emulator to
// the test topology: the harness renders an `azurite` service plus a one-shot
// `azure-init` (reusing the bench-receiver image) that creates the declared
// blob containers and same-named storage queues, then exits — the subject is
// gated on its completion, mirroring the redpanda-init pattern.
type AzureConfig struct {
	// Image is the Azurite container image (default
	// "mcr.microsoft.com/azure-storage/azurite:3.34.0").
	Image string `yaml:"image"`
	// Containers are blob containers created by azure-init. For each, a
	// same-named storage queue is created too: vmetric's azblob listener
	// polls that queue for BlobCreated events, which the generator's
	// azure_blob mode synthesizes.
	Containers []string `yaml:"containers"`
}

// ImageOrDefault returns the Azurite image.
func (a *AzureConfig) ImageOrDefault() string {
	if a != nil && a.Image != "" {
		return a.Image
	}
	return "mcr.microsoft.com/azure-storage/azurite:3.34.0"
}

// ConnectionString is the Azurite connection string as seen from the bench
// network. Every container that talks to Azurite gets it via
// AZURE_STORAGE_CONNECTION_STRING.
func (a *AzureConfig) ConnectionString() string {
	return fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://azurite:10000/%s;QueueEndpoint=http://azurite:10001/%s;",
		AzuriteAccount, AzuriteKey, AzuriteAccount, AzuriteAccount,
	)
}

// MinioConfig, when set on a TestCase (`minio:`), adds a MinIO emulator to
// the test topology: the harness renders a `minio` service plus a one-shot
// `minio-init` (reusing the bench-receiver image) that creates the declared
// buckets over the S3 API, then exits — the subject is gated on its
// completion, mirroring the azure-init pattern. MinIO is S3-compatible, so
// the s3 generator/receiver modes and the s3_sink/s3_source capabilities
// apply unchanged; only the endpoint and credentials differ from LocalStack.
type MinioConfig struct {
	// Image is the MinIO container image (default below). Pinned to a
	// release tag, never :latest.
	Image string `yaml:"image"`
	// Buckets are S3 buckets created by minio-init before the subject starts.
	Buckets []string `yaml:"buckets"`
}

// ImageOrDefault returns the MinIO image.
func (m *MinioConfig) ImageOrDefault() string {
	if m != nil && m.Image != "" {
		return m.Image
	}
	return "minio/minio:RELEASE.2025-04-22T22-12-26Z"
}

// EndpointURL is the MinIO S3 endpoint as seen from the bench network.
func (m *MinioConfig) EndpointURL() string { return "http://minio:9000" }

// UsesAWS reports whether the case adds a LocalStack emulator to the topology.
func (tc *TestCase) UsesAWS() bool { return tc.AWS != nil }

// UsesMinio reports whether the case adds a MinIO emulator to the topology.
func (tc *TestCase) UsesMinio() bool { return tc.Minio != nil }

// UsesAzure reports whether the case adds an Azurite emulator to the topology.
func (tc *TestCase) UsesAzure() bool { return tc.Azure != nil }

// IsCloudPollingReceiverMode reports whether a receiver mode drains an
// emulator by polling instead of listening — those receivers must wait for
// the emulator to be up, and arrival timestamps carry poll-interval
// granularity (so rate_ceiling must not be combined with them).
func IsCloudPollingReceiverMode(mode string) bool {
	switch mode {
	case "s3", "azure_blob", "sqs", "kinesis", "cloudwatch":
		return true
	}
	return false
}

// cloudResourceName guards every resource name that is rendered into the
// LocalStack init shell script (and Azurite init env): the charset excludes
// quotes, spaces, and shell metacharacters, so case YAML cannot inject into
// the script. It is intentionally stricter than what the real services allow.
var cloudResourceName = regexp.MustCompile(`^[a-zA-Z0-9/][a-zA-Z0-9._/-]*$`)

func validateCloudName(caseName, kind, name string) error {
	if name == "" {
		return fmt.Errorf("case %q: %s name must not be empty", caseName, kind)
	}
	if !cloudResourceName.MatchString(name) {
		return fmt.Errorf("case %q: %s name %q contains characters outside [a-zA-Z0-9._/-]", caseName, kind, name)
	}
	return nil
}

// validateCloud runs the `aws:`/`azure:` structural checks called from
// TestCase.Validate.
func (tc *TestCase) validateCloud() error {
	// AWS (LocalStack) and MinIO both own the S3 endpoint and credentials;
	// a case must pick one.
	if tc.AWS != nil && tc.Minio != nil {
		return fmt.Errorf("case %q: `aws:` and `minio:` are mutually exclusive (both provide the S3 endpoint)", tc.Name)
	}
	if tc.AWS != nil {
		if err := tc.validateAWS(); err != nil {
			return err
		}
	}
	if tc.Minio != nil {
		if err := tc.validateMinio(); err != nil {
			return err
		}
	}
	if tc.Azure != nil {
		if err := tc.validateAzure(); err != nil {
			return err
		}
	}

	// Cloud generator/receiver modes only make sense with the matching
	// emulator block (the orchestrator derives endpoints and init wiring
	// from it). The s3 mode is served by either LocalStack or MinIO.
	for _, g := range tc.AllGenerators() {
		switch g.Mode {
		case "s3":
			if tc.AWS == nil && tc.Minio == nil {
				return fmt.Errorf("case %q: generator mode \"s3\" requires an `aws:` or `minio:` block", tc.Name)
			}
		case "azure_blob":
			if tc.Azure == nil {
				return fmt.Errorf("case %q: generator mode \"azure_blob\" requires an `azure:` block", tc.Name)
			}
		}
	}
	for _, r := range tc.AllReceivers() {
		switch r.Mode {
		case "s3":
			if tc.AWS == nil && tc.Minio == nil {
				return fmt.Errorf("case %q: receiver mode \"s3\" requires an `aws:` or `minio:` block", tc.Name)
			}
		case "sqs", "kinesis", "cloudwatch":
			if tc.AWS == nil {
				return fmt.Errorf("case %q: receiver mode %q requires an `aws:` block", tc.Name, r.Mode)
			}
		case "azure_blob":
			if tc.Azure == nil {
				return fmt.Errorf("case %q: receiver mode \"azure_blob\" requires an `azure:` block", tc.Name)
			}
		}
	}
	return nil
}

// validateMinio runs the `minio:` structural checks: at least one bucket,
// each matching the shared cloud-resource charset, no duplicates.
func (tc *TestCase) validateMinio() error {
	if len(tc.Minio.Buckets) == 0 {
		return fmt.Errorf("case %q: `minio:` block requires at least one bucket", tc.Name)
	}
	seen := map[string]struct{}{}
	for _, b := range tc.Minio.Buckets {
		if err := validateCloudName(tc.Name, "minio bucket", b); err != nil {
			return err
		}
		if _, dup := seen[b]; dup {
			return fmt.Errorf("case %q: duplicate minio bucket %q", tc.Name, b)
		}
		seen[b] = struct{}{}
	}
	return nil
}

func (tc *TestCase) validateAWS() error {
	buckets := map[string]struct{}{}
	for _, b := range tc.AWS.Buckets {
		if err := validateCloudName(tc.Name, "aws bucket", b); err != nil {
			return err
		}
		if _, dup := buckets[b]; dup {
			return fmt.Errorf("case %q: duplicate aws bucket %q", tc.Name, b)
		}
		buckets[b] = struct{}{}
	}
	queues := map[string]struct{}{}
	for _, q := range tc.AWS.Queues {
		if err := validateCloudName(tc.Name, "aws queue", q); err != nil {
			return err
		}
		if _, dup := queues[q]; dup {
			return fmt.Errorf("case %q: duplicate aws queue %q", tc.Name, q)
		}
		queues[q] = struct{}{}
	}
	topics := map[string]struct{}{}
	for _, t := range tc.AWS.Topics {
		if err := validateCloudName(tc.Name, "aws topic", t); err != nil {
			return err
		}
		if _, dup := topics[t]; dup {
			return fmt.Errorf("case %q: duplicate aws topic %q", tc.Name, t)
		}
		topics[t] = struct{}{}
	}
	for _, s := range tc.AWS.Streams {
		if err := validateCloudName(tc.Name, "aws stream", s.Name); err != nil {
			return err
		}
		if s.Shards < 0 {
			return fmt.Errorf("case %q: aws stream %q shards must be >= 0 (0 defaults to 1), got %d", tc.Name, s.Name, s.Shards)
		}
	}
	for _, lg := range tc.AWS.LogGroups {
		if err := validateCloudName(tc.Name, "aws log group", lg.Group); err != nil {
			return err
		}
		if lg.Stream != "" {
			if err := validateCloudName(tc.Name, "aws log stream", lg.Stream); err != nil {
				return err
			}
		}
	}
	for _, n := range tc.AWS.BucketNotifications {
		if _, ok := buckets[n.Bucket]; !ok {
			return fmt.Errorf("case %q: bucket_notifications references undeclared bucket %q", tc.Name, n.Bucket)
		}
		if _, ok := queues[n.Queue]; !ok {
			return fmt.Errorf("case %q: bucket_notifications references undeclared queue %q", tc.Name, n.Queue)
		}
	}
	for _, s := range tc.AWS.Subscriptions {
		if _, ok := topics[s.Topic]; !ok {
			return fmt.Errorf("case %q: subscriptions references undeclared topic %q", tc.Name, s.Topic)
		}
		if _, ok := queues[s.Queue]; !ok {
			return fmt.Errorf("case %q: subscriptions references undeclared queue %q", tc.Name, s.Queue)
		}
	}
	for _, svc := range tc.AWS.Services {
		if err := validateCloudName(tc.Name, "aws service", svc); err != nil {
			return err
		}
	}
	for _, so := range tc.AWS.SeedObjects {
		if _, ok := buckets[so.Bucket]; !ok {
			return fmt.Errorf("case %q: seed_objects references undeclared bucket %q", tc.Name, so.Bucket)
		}
		if so.Prefix != "" {
			if err := validateCloudName(tc.Name, "aws seed prefix", so.Prefix); err != nil {
				return err
			}
		}
		if so.Marker != "" {
			if err := validateCloudName(tc.Name, "aws seed marker", so.Marker); err != nil {
				return err
			}
		}
		if so.Objects <= 0 {
			return fmt.Errorf("case %q: seed_objects for bucket %q requires objects > 0, got %d", tc.Name, so.Bucket, so.Objects)
		}
		if so.Lines <= 0 {
			return fmt.Errorf("case %q: seed_objects for bucket %q requires lines > 0, got %d", tc.Name, so.Bucket, so.Lines)
		}
	}
	return nil
}

// azureContainerName mirrors Azure's blob container naming rules (lowercase
// alphanumerics and dashes, 3-63 chars) — Azurite enforces them too, so
// failing at parse time beats failing inside azure-init.
var azureContainerName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)

func (tc *TestCase) validateAzure() error {
	if len(tc.Azure.Containers) == 0 {
		return fmt.Errorf("case %q: `azure:` block requires at least one container", tc.Name)
	}
	seen := map[string]struct{}{}
	for _, c := range tc.Azure.Containers {
		if !azureContainerName.MatchString(c) {
			return fmt.Errorf("case %q: azure container %q must match %s", tc.Name, c, azureContainerName)
		}
		if _, dup := seen[c]; dup {
			return fmt.Errorf("case %q: duplicate azure container %q", tc.Name, c)
		}
		seen[c] = struct{}{}
	}
	return nil
}
