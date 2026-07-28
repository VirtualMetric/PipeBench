package config

import (
	"strings"
	"testing"
)

func TestValidateCloud(t *testing.T) {
	tests := []struct {
		name    string
		tc      TestCase
		wantErr string // substring of the expected error, "" = valid
	}{
		{
			name: "aws block with full wiring",
			tc: TestCase{
				Name: "c",
				AWS: &AWSConfig{
					Buckets: []string{"bench-in"},
					Queues:  []string{"bench-events"},
					Topics:  []string{"bench-topic"},
					Streams: []AWSStream{{Name: "bench-stream", Shards: 4}},
					LogGroups: []AWSLogGroup{
						{Group: "/bench/group", Stream: "bench"},
					},
					BucketNotifications: []AWSBucketNotification{
						{Bucket: "bench-in", Queue: "bench-events"},
					},
					Subscriptions: []AWSSubscription{
						{Topic: "bench-topic", Queue: "bench-events"},
					},
				},
				Generator: GeneratorConfig{Mode: "s3", Target: "http://localstack:4566"},
			},
		},
		{
			name: "azure block with container",
			tc: TestCase{
				Name:      "c",
				Azure:     &AzureConfig{Containers: []string{"bench-in"}},
				Generator: GeneratorConfig{Mode: "azure_blob", Target: "http://azurite:10000"},
			},
		},
		{
			name: "s3 generator without aws block",
			tc: TestCase{
				Name:      "c",
				Generator: GeneratorConfig{Mode: "s3", Target: "http://localstack:4566"},
			},
			wantErr: "requires an `aws:` or `minio:` block",
		},
		{
			name: "minio block with bucket and s3 receiver",
			tc: TestCase{
				Name:     "c",
				Minio:    &MinioConfig{Buckets: []string{"bench-out"}},
				Receiver: ReceiverConfig{Mode: "s3"},
			},
		},
		{
			name: "aws and minio mutually exclusive",
			tc: TestCase{
				Name:  "c",
				AWS:   &AWSConfig{Buckets: []string{"bench-out"}},
				Minio: &MinioConfig{Buckets: []string{"bench-out"}},
			},
			wantErr: "mutually exclusive",
		},
		{
			name: "minio block without buckets",
			tc: TestCase{
				Name:  "c",
				Minio: &MinioConfig{},
			},
			wantErr: "at least one bucket",
		},
		{
			name: "minio bucket name charset enforced",
			tc: TestCase{
				Name:  "c",
				Minio: &MinioConfig{Buckets: []string{"bad;rm -rf /"}},
			},
			wantErr: "contains characters outside",
		},
		{
			name: "minio endpoint name reserved",
			tc: TestCase{
				Name:      "c",
				Minio:     &MinioConfig{Buckets: []string{"bench-out"}},
				Endpoints: []Endpoint{{Name: "minio", Image: "img"}},
			},
			wantErr: "reserved",
		},
		{
			name: "redpanda endpoint name reserved",
			tc: TestCase{
				Name:      "c",
				Endpoints: []Endpoint{{Name: "redpanda", Image: "img"}},
			},
			wantErr: "reserved",
		},
		{
			name: "azure_blob receiver without azure block",
			tc: TestCase{
				Name:     "c",
				Receiver: ReceiverConfig{Mode: "azure_blob"},
			},
			wantErr: "requires an `azure:` block",
		},
		{
			name: "sqs receiver without aws block",
			tc: TestCase{
				Name:     "c",
				Receiver: ReceiverConfig{Mode: "sqs"},
			},
			wantErr: "requires an `aws:` block",
		},
		{
			name: "notification references undeclared bucket",
			tc: TestCase{
				Name: "c",
				AWS: &AWSConfig{
					Queues:              []string{"q"},
					BucketNotifications: []AWSBucketNotification{{Bucket: "nope", Queue: "q"}},
				},
			},
			wantErr: "undeclared bucket",
		},
		{
			name: "subscription references undeclared queue",
			tc: TestCase{
				Name: "c",
				AWS: &AWSConfig{
					Topics:        []string{"t"},
					Subscriptions: []AWSSubscription{{Topic: "t", Queue: "nope"}},
				},
			},
			wantErr: "undeclared queue",
		},
		{
			name: "shell metacharacters rejected in resource names",
			tc: TestCase{
				Name: "c",
				AWS:  &AWSConfig{Buckets: []string{"bench;rm -rf /"}},
			},
			wantErr: "contains characters outside",
		},
		{
			name: "azure block without containers",
			tc: TestCase{
				Name:  "c",
				Azure: &AzureConfig{},
			},
			wantErr: "at least one container",
		},
		{
			name: "azure container naming rules enforced",
			tc: TestCase{
				Name:  "c",
				Azure: &AzureConfig{Containers: []string{"Bad_Name"}},
			},
			wantErr: "must match",
		},
		{
			name: "endpoint name colliding with emulator service rejected",
			tc: TestCase{
				Name:      "c",
				Endpoints: []Endpoint{{Name: "localstack", Image: "img"}},
			},
			wantErr: "reserved",
		},
		{
			name: "negative stream shards rejected",
			tc: TestCase{
				Name: "c",
				AWS:  &AWSConfig{Streams: []AWSStream{{Name: "s", Shards: -1}}},
			},
			wantErr: "shards must be >= 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tc.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestAWSConfigDefaults(t *testing.T) {
	aws := &AWSConfig{
		Buckets: []string{"b"},
		Queues:  []string{"q"},
	}
	if got, want := aws.ImageOrDefault(), "localstack/localstack:4"; got != want {
		t.Errorf("ImageOrDefault() = %q, want %q", got, want)
	}
	if got, want := aws.RegionOrDefault(), "us-east-1"; got != want {
		t.Errorf("RegionOrDefault() = %q, want %q", got, want)
	}
	if got, want := aws.ServicesOrDefault(), "s3,sqs,sts"; got != want {
		t.Errorf("ServicesOrDefault() = %q, want %q", got, want)
	}
	if got, want := aws.QueueURL("q"), "http://localstack:4566/000000000000/q"; got != want {
		t.Errorf("QueueURL() = %q, want %q", got, want)
	}
	if got, want := aws.QueueARN("q"), "arn:aws:sqs:us-east-1:000000000000:q"; got != want {
		t.Errorf("QueueARN() = %q, want %q", got, want)
	}
	if got, want := aws.TopicARN("t"), "arn:aws:sns:us-east-1:000000000000:t"; got != want {
		t.Errorf("TopicARN() = %q, want %q", got, want)
	}
}

func TestValidateAWSSeedObjects(t *testing.T) {
	base := func(so AWSSeedObjects) *TestCase {
		return &TestCase{
			Name:     "seed-case",
			Type:     "correctness",
			Duration: "10s",
			AWS: &AWSConfig{
				Buckets:     []string{"bench-in"},
				SeedObjects: []AWSSeedObjects{so},
			},
			Receiver:    ReceiverConfig{Mode: "tcp", Listen: ":9001"},
			Correctness: CorrectnessConfig{},
		}
	}

	tests := []struct {
		name    string
		so      AWSSeedObjects
		wantErr bool
	}{
		{name: "valid", so: AWSSeedObjects{Bucket: "bench-in", Objects: 10, Lines: 100}},
		{name: "valid with prefix and marker", so: AWSSeedObjects{Bucket: "bench-in", Prefix: "seed/", Objects: 1, Lines: 1, Marker: "SEED"}},
		{name: "undeclared bucket", so: AWSSeedObjects{Bucket: "nope", Objects: 1, Lines: 1}, wantErr: true},
		{name: "zero objects", so: AWSSeedObjects{Bucket: "bench-in", Objects: 0, Lines: 1}, wantErr: true},
		{name: "zero lines", so: AWSSeedObjects{Bucket: "bench-in", Objects: 1, Lines: 0}, wantErr: true},
		{name: "injecting marker", so: AWSSeedObjects{Bucket: "bench-in", Objects: 1, Lines: 1, Marker: "a'; rm -rf /"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := base(tt.so).validateAWS()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateAWS() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMinioConfigDefaults(t *testing.T) {
	m := &MinioConfig{Buckets: []string{"bench-out"}}
	if got, want := m.ImageOrDefault(), "minio/minio:RELEASE.2025-04-22T22-12-26Z"; got != want {
		t.Errorf("ImageOrDefault() = %q, want %q", got, want)
	}
	if got, want := m.EndpointURL(), "http://minio:9000"; got != want {
		t.Errorf("EndpointURL() = %q, want %q", got, want)
	}
}

func TestAzureConnectionString(t *testing.T) {
	az := &AzureConfig{Containers: []string{"bench"}}
	cs := az.ConnectionString()
	for _, want := range []string{
		"AccountName=devstoreaccount1",
		"BlobEndpoint=http://azurite:10000/devstoreaccount1",
		"QueueEndpoint=http://azurite:10001/devstoreaccount1",
	} {
		if !strings.Contains(cs, want) {
			t.Errorf("ConnectionString() missing %q in %q", want, cs)
		}
	}
}
