package config

import "testing"

// TestCloneForRunIsolatesMutatedFields pins the invariant the CLI relies on:
// each (case, subject) pair runs against its own copy, so the runner rewriting
// `${NAME}` into an endpoint command — or seeding a rotated cert into
// vault.secrets — cannot leak into the next subject's run.
func TestCloneForRunIsolatesMutatedFields(t *testing.T) {
	orig := &TestCase{
		Name: "resolve-case",
		Endpoints: []Endpoint{{
			Name:    "device",
			Command: []string{"send", "--token", "${TOKEN}"},
			Env:     map[string]string{"TOKEN": "${TOKEN}"},
		}},
		Vault: &VaultConfig{Secrets: map[string]map[string]string{
			"tls": {"cert": "original"},
		}},
	}

	cp := orig.CloneForRun()

	// Same rewrites the runner performs, applied to the copy only.
	cp.Endpoints[0].Command[2] = "subject-a-token"
	cp.Endpoints[0].Env["TOKEN"] = "subject-a-token"
	cp.Vault.Secrets["tls"]["cert"] = "rotated"

	if got := orig.Endpoints[0].Command[2]; got != "${TOKEN}" {
		t.Errorf("original endpoint command mutated: got %q, want %q", got, "${TOKEN}")
	}
	if got := orig.Endpoints[0].Env["TOKEN"]; got != "${TOKEN}" {
		t.Errorf("original endpoint env mutated: got %q, want %q", got, "${TOKEN}")
	}
	if got := orig.Vault.Secrets["tls"]["cert"]; got != "original" {
		t.Errorf("original vault secret mutated: got %q, want %q", got, "original")
	}
	if cp.Name != orig.Name {
		t.Errorf("clone lost scalar fields: got name %q, want %q", cp.Name, orig.Name)
	}
}

// TestCloneForRunHandlesEmptyCase covers the nil/empty shapes most cases have —
// no endpoints and no vault block — where the clone must not invent one.
func TestCloneForRunHandlesEmptyCase(t *testing.T) {
	if got := (*TestCase)(nil).CloneForRun(); got != nil {
		t.Fatalf("nil case cloned to %v, want nil", got)
	}
	cp := (&TestCase{Name: "bare"}).CloneForRun()
	if cp.Endpoints != nil {
		t.Errorf("endpoints became %v, want nil", cp.Endpoints)
	}
	if cp.Vault != nil {
		t.Errorf("vault became %v, want nil", cp.Vault)
	}
}
