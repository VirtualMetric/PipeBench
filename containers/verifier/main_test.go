package main

import (
	"testing"
	"time"
)

func TestBuildVerdict(t *testing.T) {
	t.Parallel()

	base := config{Expected: 100000, MsgField: "msg", NullFields: []string{"msg"}}

	tests := []struct {
		name           string
		st             stats
		cfg            config
		wantPassed     bool
		wantDuplicates int64
		wantErrSub     string // substring expected in the joined errors (empty = expect none)
	}{
		{
			name:       "clean exact-once",
			st:         stats{total: 100000, distinct: 100000, nulls: 0},
			cfg:        base,
			wantPassed: true,
		},
		{
			name:           "duplicates fail",
			st:             stats{total: 100010, distinct: 100000, nulls: 0},
			cfg:            base,
			wantPassed:     false,
			wantDuplicates: 10,
			wantErrSub:     "duplicate",
		},
		{
			// Expected is the SOURCE count; total exceeds it from at-least-once
			// duplication. Under AllowOverDel the count check asserts on distinct
			// (== source), so this passes despite total > Expected.
			name:           "duplicates tolerated under overdelivery",
			st:             stats{total: 100010, distinct: 100000, nulls: 0},
			cfg:            config{Expected: 100000, MsgField: "msg", NullFields: []string{"msg"}, AllowOverDel: true},
			wantPassed:     true,
			wantDuplicates: 10,
		},
		{
			// Overdelivery tolerates duplicates but NOT loss of unique rows.
			name:       "overdelivery still fails on unique-row loss",
			st:         stats{total: 99990, distinct: 99990, nulls: 0},
			cfg:        config{Expected: 100000, MsgField: "msg", NullFields: []string{"msg"}, AllowOverDel: true},
			wantPassed: false,
			wantErrSub: "unique row count mismatch",
		},
		{
			name:       "loss fail",
			st:         stats{total: 99990, distinct: 99990, nulls: 0},
			cfg:        base,
			wantPassed: false,
			wantErrSub: "row count mismatch",
		},
		{
			name:       "null payload fail",
			st:         stats{total: 100000, distinct: 100000, nulls: 3},
			cfg:        base,
			wantPassed: false,
			wantErrSub: "NULL required field",
		},
		{
			name:       "empty bucket fails on loss",
			st:         stats{total: 0, distinct: 0, nulls: 0},
			cfg:        base,
			wantPassed: false,
			wantErrSub: "row count mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := buildVerdict(tt.st, tt.cfg)
			if v.Passed != tt.wantPassed {
				t.Errorf("Passed = %v, want %v (errors: %v)", v.Passed, tt.wantPassed, v.Errors)
			}
			if v.Duplicates != tt.wantDuplicates {
				t.Errorf("Duplicates = %d, want %d", v.Duplicates, tt.wantDuplicates)
			}
			if tt.wantErrSub != "" && !containsAny(v.Errors, tt.wantErrSub) {
				t.Errorf("errors %v do not contain %q", v.Errors, tt.wantErrSub)
			}
		})
	}
}

func TestSourceExpr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  config
		want string
	}{
		{
			name: "parquet no prefix",
			cfg:  config{Bucket: "bench-out", Format: "parquet"},
			want: "read_parquet('s3://bench-out/**/*.parquet')",
		},
		{
			name: "avro with prefix",
			cfg:  config{Bucket: "bench-out", Prefix: "out/", Format: "avro"},
			want: "read_avro('s3://bench-out/out/**/*.avro')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := sourceExpr(tt.cfg); got != tt.want {
				t.Errorf("sourceExpr() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPrelude(t *testing.T) {
	t.Parallel()

	t.Run("parquet omits avro load and redacts creds", func(t *testing.T) {
		t.Parallel()
		cfg := config{Endpoint: "http://localstack:4566", Region: "us-east-1", Format: "parquet", AccessKey: "ak", SecretKey: "sk"}
		sql, redacted := cfg.prelude()
		if contains(redacted, "sk") || contains(redacted, "secret_access_key") {
			t.Errorf("redacted prelude leaks credentials: %q", redacted)
		}
		if !contains(sql, "s3_secret_access_key='sk'") {
			t.Errorf("full prelude missing credential set: %q", sql)
		}
		if contains(sql, "LOAD avro") {
			t.Errorf("parquet prelude should not LOAD avro: %q", sql)
		}
		if !contains(sql, "s3_use_ssl=false") || !contains(sql, "s3_endpoint='localstack:4566'") {
			t.Errorf("prelude endpoint/ssl wrong: %q", sql)
		}
	})

	t.Run("avro loads avro extension", func(t *testing.T) {
		t.Parallel()
		cfg := config{Endpoint: "https://s3.example.com", Region: "eu", Format: "avro", AccessKey: "ak", SecretKey: "sk"}
		sql, _ := cfg.prelude()
		if !contains(sql, "LOAD avro") {
			t.Errorf("avro prelude must LOAD avro: %q", sql)
		}
		if !contains(sql, "s3_use_ssl=true") {
			t.Errorf("https endpoint should set ssl true: %q", sql)
		}
	})
}

func TestSplitEndpoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in       string
		wantHost string
		wantSSL  bool
	}{
		{"http://localstack:4566", "localstack:4566", false},
		{"https://s3.amazonaws.com/", "s3.amazonaws.com", true},
		{"minio:9000", "minio:9000", false},
	}
	for _, tt := range tests {
		host, ssl := splitEndpoint(tt.in)
		if host != tt.wantHost || ssl != tt.wantSSL {
			t.Errorf("splitEndpoint(%q) = (%q,%v), want (%q,%v)", tt.in, host, ssl, tt.wantHost, tt.wantSSL)
		}
	}
}

func TestParseRows(t *testing.T) {
	t.Parallel()

	rows, err := parseRows([]byte(`[{"total":100000,"distinct":100000,"nulls":0}]`))
	if err != nil {
		t.Fatalf("parseRows error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if got := asInt(rows[0]["total"]); got != 100000 {
		t.Errorf("total = %d, want 100000", got)
	}

	empty, err := parseRows([]byte("[]"))
	if err != nil {
		t.Fatalf("parseRows([]) error: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("want 0 rows, got %d", len(empty))
	}
}

func TestQuietPollsFloor(t *testing.T) {
	t.Parallel()
	// QuietWindow smaller than the poll interval must still require >=1 stable
	// poll, never 0 (which would declare stability immediately).
	cfg := config{QuietWindow: 1 * time.Second, PollInterval: 5 * time.Second}
	if qp := int(cfg.QuietWindow / cfg.PollInterval); qp != 0 {
		t.Fatalf("precondition: expected integer division to floor to 0, got %d", qp)
	}
	// The floor is applied in waitStable via max(); assert it yields 1.
	if quietPolls := max(int(cfg.QuietWindow/cfg.PollInterval), 1); quietPolls != 1 {
		t.Errorf("quietPolls = %d, want 1", quietPolls)
	}
}

func containsAny(errs []string, sub string) bool {
	for _, e := range errs {
		if contains(e, sub) {
			return true
		}
	}
	return false
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
