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
	},
}
