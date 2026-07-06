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
		{name: "nil database block is a no-op", mutate: func(tc *TestCase) { tc.Database = nil }},
		{
			name:    "unknown engine",
			mutate:  func(tc *TestCase) { tc.Database.Engine = "oracle" },
			wantErr: `unknown database.engine "oracle"`,
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
