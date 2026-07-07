package config

import "fmt"

// DatabaseEngine supplies everything the generic `database:` compose block
// needs to run a specific engine: the default image/credential, the env vars
// the image expects, a healthcheck command using a CLI already shipped in
// the image, and the one-shot init command that creates the database and
// runs the seed file mounted at /db-seed/init.sql. Adding a new engine
// (mysql, postgresql, ...) is exactly one new entry in DatabaseEngines — no
// orchestrator or compose-template changes required.
type DatabaseEngine struct {
	DefaultImage    string
	DefaultPassword string
	BuildEnv        func(password string) map[string]string
	BuildHealthCmd  func(password string) string
	BuildInitCmd    func(password, database string) string

	// TLS wiring (used only when a case sets `database.tls: true`). The
	// orchestrator generates a CA + RSA server cert and bind-mounts server.crt
	// / server.key at TLSServerCertPath / TLSServerKeyPath in the container;
	// BuildTLSConf returns a config file (mounted at its returned path) that
	// points the engine at those cert files so it terminates TLS with them. An
	// empty TLSServerCertPath means the engine does not support this mechanism.
	TLSServerCertPath string
	TLSServerKeyPath  string
	BuildTLSConf      func() (mountPath, content string)

	// BuildTLSCommand, when non-nil and the case enables TLS, overrides the
	// database container's entrypoint with `/bin/sh -c <returned string>`.
	// Engines whose image auto-reads the BuildTLSConf file (mssql via mssql.conf,
	// mysql via /etc/mysql/conf.d) leave this nil and use the image defaults.
	// Postgres needs it because it (a) refuses to start with a group/world-
	// readable TLS key — the harness writes server.key as 0644 — and (b) has no
	// auto-include conf dir, so TLS must be turned on via `-c` args. certPath /
	// keyPath are the in-container mount paths (TLSServerCertPath/KeyPath).
	BuildTLSCommand func(certPath, keyPath string) string
}

// DatabaseEngines is the registry of engines the `database:` case block can
// select via `engine:`.
var DatabaseEngines = map[string]DatabaseEngine{
	"mssql": {
		DefaultImage: "mcr.microsoft.com/mssql/server:2022-latest",
		// 8+ chars, upper+lower+digit+symbol — satisfies MSSQL's SA
		// password complexity policy. Test-only credential.
		DefaultPassword: "PipeBench-Db1!",
		BuildEnv: func(password string) map[string]string {
			return map[string]string{
				"ACCEPT_EULA":       "Y",
				"MSSQL_PID":         "Developer",
				"MSSQL_SA_PASSWORD": password,
			}
		},
		BuildHealthCmd: func(password string) string {
			// -C trusts the container's self-signed cert for this internal
			// probe only — orthogonal to a case's own TLS verification
			// setting under test on the director's connection. Two escaping
			// layers apply here: (1) `$$` instead of `$` — docker compose
			// itself interpolates `$VAR` in compose file values at parse
			// time, so a bare `$MSSQL_SA_PASSWORD` would resolve against
			// the HOST environment (usually unset -> blank) before the
			// container's shell ever sees it; `$$` escapes to a literal `$`
			// that compose passes through for the container's own `sh -c`
			// to expand against its `environment:` block. (2) `\"` instead
			// of `"` — this string is rendered inside a double-quoted YAML
			// flow scalar (`["CMD-SHELL", "..."]`), so every literal `"`
			// must be YAML-escaped or it terminates the scalar early and
			// corrupts the generated compose file.
			return `/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P \"$$MSSQL_SA_PASSWORD\" -C -Q 'SELECT 1' -b || exit 1`
		},
		BuildInitCmd: func(password, database string) string {
			// CREATE DATABASE must be the only statement in its batch,
			// hence two separate sqlcmd invocations. Same `$$`/`\"`
			// escaping requirement as BuildHealthCmd (rendered into
			// "{{ ... }}" in the compose template).
			return fmt.Sprintf(
				`set -e; /opt/mssql-tools18/bin/sqlcmd -S database -U sa -P \"$$MSSQL_SA_PASSWORD\" -C -Q \"CREATE DATABASE %s\"; `+
					`/opt/mssql-tools18/bin/sqlcmd -S database -U sa -P \"$$MSSQL_SA_PASSWORD\" -d %s -C -i /db-seed/init.sql; `+
					`echo database seeding complete`,
				database, database)
		},
		// SQL Server 2022 (Linux) reads its TLS cert/key from mssql.conf at
		// startup. We mount the generated RSA server.crt/server.key at these
		// paths and a mssql.conf that points at them. forceencryption=0 leaves
		// TLS optional server-side (the healthcheck's plain sqlcmd -C still
		// connects) while the director's encrypt=true still negotiates TLS and
		// verifies the cert against the mounted CA.
		TLSServerCertPath: "/etc/ssl/mssql/server.crt",
		TLSServerKeyPath:  "/etc/ssl/mssql/server.key",
		BuildTLSConf: func() (string, string) {
			return "/var/opt/mssql/mssql.conf", "[network]\n" +
				"tlscert = /etc/ssl/mssql/server.crt\n" +
				"tlskey = /etc/ssl/mssql/server.key\n" +
				"tlsprotocols = 1.2\n" +
				"forceencryption = 0\n"
		},
	},
	"postgres": {
		DefaultImage: "postgres:17",
		// pg has no password-complexity policy; reuse the mssql credential for
		// parity across engines. Test-only credential.
		DefaultPassword: "PipeBench-Db1!",
		BuildEnv: func(password string) map[string]string {
			// The official image only auto-creates the `postgres` superuser;
			// the bench db is created by BuildInitCmd. POSTGRES_PASSWORD is
			// required for TCP auth from database-init and the subject.
			return map[string]string{
				"POSTGRES_PASSWORD": password,
			}
		},
		BuildHealthCmd: func(password string) string {
			// pg_isready checks the server accepts connections; it needs no
			// auth, so no password/escaping is involved here.
			return `pg_isready -U postgres -d postgres || exit 1`
		},
		BuildInitCmd: func(password, database string) string {
			// CREATE DATABASE cannot run inside a transaction, so it is its own
			// psql -c invocation, then the seed file runs against it. Same
			// `$$`/`\"` escaping requirement as the mssql init (rendered into
			// "{{ ... }}" in the compose template): `$$` survives compose's own
			// $VAR interpolation to reach the container shell as a literal `$`;
			// `\"` survives the double-quoted YAML flow scalar. ON_ERROR_STOP
			// makes psql fail the container on any SQL error.
			return fmt.Sprintf(
				`set -e; PGPASSWORD=\"$$POSTGRES_PASSWORD\" psql -h database -U postgres -v ON_ERROR_STOP=1 -c \"CREATE DATABASE %s\"; `+
					`PGPASSWORD=\"$$POSTGRES_PASSWORD\" psql -h database -U postgres -d %s -v ON_ERROR_STOP=1 -f /db-seed/init.sql; `+
					`echo database seeding complete`,
				database, database)
		},
		TLSServerCertPath: "/etc/pg/certs/server.crt",
		TLSServerKeyPath:  "/etc/pg/certs/server.key",
		// Postgres drives TLS via -c args (BuildTLSCommand), not a config file,
		// so this returns an inert mount that keeps the orchestrator's
		// unconditional conf-mount valid without special-casing. Postgres never
		// reads this path.
		BuildTLSConf: func() (string, string) {
			return "/etc/pg/pipebench-unused.conf",
				"# PipeBench: postgres TLS is configured via -c args; this file is unused\n"
		},
		BuildTLSCommand: func(certPath, keyPath string) string {
			// Copy the world-readable (0644) mounted key to a postgres-owned
			// 0600 copy — postgres refuses to start with a group/world-readable
			// key — then start postgres with TLS on (no auto-include conf dir,
			// so ssl settings are passed via -c). Runs as the image's default
			// root user; docker-entrypoint.sh then steps down to the postgres
			// user, which reads the 0600 copy.
			const keyCopy = "/var/lib/postgresql/pipebench-server.key"
			return fmt.Sprintf(
				"install -m 600 -o postgres -g postgres %s %s && "+
					"exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=%s -c ssl_key_file=%s",
				keyPath, keyCopy, certPath, keyCopy)
		},
	},
}
