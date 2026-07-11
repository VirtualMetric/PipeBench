package config

import (
	"strings"
	"testing"
)

func TestValidateDatabase(t *testing.T) {
	base := func() *TestCase {
		return &TestCase{
			Name: "database_case",
			Database: &DatabaseConfig{
				Engine:  "mssql",
				SeedSQL: "CREATE TABLE t (id INT);",
			},
		}
	}

	tests := []struct {
		name    string
		mutate  func(tc *TestCase)
		wantErr string // substring of the expected error, "" = valid
	}{
		{name: "valid mssql database block", mutate: func(*TestCase) {}},
		{name: "valid oracle database block", mutate: func(tc *TestCase) { tc.Database.Engine = "oracle" }},
		{name: "nil database block is a no-op", mutate: func(tc *TestCase) { tc.Database = nil }},
		{
			name:    "unknown engine",
			mutate:  func(tc *TestCase) { tc.Database.Engine = "nosuchdb" },
			wantErr: `unknown database.engine "nosuchdb"`,
		},
		{
			name:    "missing seed_sql",
			mutate:  func(tc *TestCase) { tc.Database.SeedSQL = "" },
			wantErr: "requires `seed_sql`",
		},
		{
			name:    "database name with shell metacharacter",
			mutate:  func(tc *TestCase) { tc.Database.Database = "bench; rm -rf /" },
			wantErr: "must match",
		},
		{name: "custom database name", mutate: func(tc *TestCase) { tc.Database.Database = "bench_2" }},
		{
			name:    "tls on a TLS-less engine is rejected",
			mutate:  func(tc *TestCase) { tc.Database.Engine = "oracle"; tc.Database.TLS = true },
			wantErr: `not supported by engine "oracle"`,
		},
		{name: "tls on a TLS-capable engine is allowed", mutate: func(tc *TestCase) { tc.Database.TLS = true }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tc := base()
			tt.mutate(tc)
			err := tc.validateDatabase()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateDatabase() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateDatabase() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestOracleEngineEntry(t *testing.T) {
	engine, ok := DatabaseEngines["oracle"]
	if !ok {
		t.Fatal("oracle engine not registered in DatabaseEngines")
	}
	if engine.DefaultImage != "gvenzl/oracle-free:23-slim-faststart" {
		t.Errorf("DefaultImage = %q", engine.DefaultImage)
	}
	if env := engine.BuildEnv("pw"); env["ORACLE_PASSWORD"] != "pw" {
		t.Errorf("BuildEnv ORACLE_PASSWORD = %q, want pw", env["ORACLE_PASSWORD"])
	}
	if hc := engine.BuildHealthCmd("pw"); hc != "healthcheck.sh" {
		t.Errorf("BuildHealthCmd = %q, want healthcheck.sh", hc)
	}
	init := engine.BuildInitCmd("pw", "bench")
	// Password must never be string-built into the command — only the env ref.
	if strings.Contains(init, "pw") {
		t.Errorf("BuildInitCmd leaks the raw password: %q", init)
	}
	for _, want := range []string{"$$ORACLE_PASSWORD", "/db-seed/init.sql", "sqlplus", "FREEPDB1"} {
		if !strings.Contains(init, want) {
			t.Errorf("BuildInitCmd missing %q: %q", want, init)
		}
	}
}

func TestDatabaseConfigOrDefaults(t *testing.T) {
	engine := DatabaseEngines["mssql"]

	t.Run("nil config falls back to engine defaults", func(t *testing.T) {
		var d *DatabaseConfig
		if got := d.ImageOrDefault(engine); got != engine.DefaultImage {
			t.Errorf("ImageOrDefault() = %q, want %q", got, engine.DefaultImage)
		}
		if got := d.PasswordOrDefault(engine); got != engine.DefaultPassword {
			t.Errorf("PasswordOrDefault() = %q, want %q", got, engine.DefaultPassword)
		}
		if got := d.DatabaseOrDefault(); got != "bench" {
			t.Errorf(`DatabaseOrDefault() = %q, want "bench"`, got)
		}
	})

	t.Run("explicit values override the engine defaults", func(t *testing.T) {
		d := &DatabaseConfig{Image: "custom:latest", Password: "custom-pass", Database: "custom_db"}
		if got := d.ImageOrDefault(engine); got != "custom:latest" {
			t.Errorf("ImageOrDefault() = %q, want custom:latest", got)
		}
		if got := d.PasswordOrDefault(engine); got != "custom-pass" {
			t.Errorf("PasswordOrDefault() = %q, want custom-pass", got)
		}
		if got := d.DatabaseOrDefault(); got != "custom_db" {
			t.Errorf("DatabaseOrDefault() = %q, want custom_db", got)
		}
	})
}
