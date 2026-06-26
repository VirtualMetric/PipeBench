package runner

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestSetAllowedIPs covers the in-place allowlist patch used by the ACL-rotation
// driver: it must replace acl.allowed_ips wherever an `acl:` mapping appears
// (nested under environments/nodes/properties), leave everything else untouched,
// and report how many blocks it patched so the driver can fail a no-op rotation.
func TestSetAllowedIPs(t *testing.T) {
	const cfg = `
listen:
  httpconfigport: 8080
environments:
  - name: "1"
    nodes:
      - name: "1"
        properties:
          acl:
            allowed_ips:
              - "192.0.2.1"
            access_tokens: []
            update_interval: 3
          metadata:
            config_id: "keep-me"
targets:
  - name: tcp-out
    properties:
      port: 9001
`
	var root map[string]any
	if err := yaml.Unmarshal([]byte(cfg), &root); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	want := []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	if n := setAllowedIPs(root, want); n != 1 {
		t.Fatalf("patched %d acl blocks, want 1", n)
	}

	// Round-trip and re-read to confirm the new list landed and nothing else moved.
	out, err := yaml.Marshal(root)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := yaml.Unmarshal(out, &got); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}

	node := got["environments"].([]any)[0].(map[string]any)["nodes"].([]any)[0].(map[string]any)
	props := node["properties"].(map[string]any)
	acl := props["acl"].(map[string]any)

	gotIPs := acl["allowed_ips"].([]any)
	if len(gotIPs) != len(want) {
		t.Fatalf("allowed_ips len = %d, want %d", len(gotIPs), len(want))
	}
	for i, ip := range want {
		if gotIPs[i] != ip {
			t.Errorf("allowed_ips[%d] = %v, want %s", i, gotIPs[i], ip)
		}
	}

	// Sibling keys under acl and elsewhere must survive untouched.
	if acl["update_interval"] != 3 {
		t.Errorf("update_interval changed: %v", acl["update_interval"])
	}
	if _, ok := acl["access_tokens"]; !ok {
		t.Error("access_tokens dropped from acl block")
	}
	if props["metadata"].(map[string]any)["config_id"] != "keep-me" {
		t.Error("metadata.config_id changed")
	}
	if got["listen"].(map[string]any)["httpconfigport"] != 8080 {
		t.Errorf("listen.httpconfigport changed: %v", got["listen"].(map[string]any)["httpconfigport"])
	}
}

// TestSetAllowedIPsNoACL: a config with no acl block must report zero patches so
// the driver can surface a no-op rotation instead of silently doing nothing.
func TestSetAllowedIPsNoACL(t *testing.T) {
	const cfg = `
listen:
  httpconfigport: 8080
targets:
  - name: tcp-out
    properties:
      port: 9001
`
	var root map[string]any
	if err := yaml.Unmarshal([]byte(cfg), &root); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	before, _ := yaml.Marshal(root)
	if n := setAllowedIPs(root, []string{"10.0.0.0/8"}); n != 0 {
		t.Fatalf("patched %d acl blocks, want 0", n)
	}
	after, _ := yaml.Marshal(root)
	if !reflect.DeepEqual(before, after) {
		t.Error("config mutated despite having no acl block")
	}
}

// TestSetAllowedIPsMultiple: every acl mapping in the tree is patched (e.g. a
// cluster-level and a node-level acl) and counted.
func TestSetAllowedIPsMultiple(t *testing.T) {
	const cfg = `
environments:
  - clusters:
      - properties:
          acl:
            allowed_ips: ["192.0.2.1"]
    nodes:
      - properties:
          acl:
            allowed_ips: ["192.0.2.2"]
`
	var root map[string]any
	if err := yaml.Unmarshal([]byte(cfg), &root); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if n := setAllowedIPs(root, []string{"10.0.0.0/8"}); n != 2 {
		t.Fatalf("patched %d acl blocks, want 2", n)
	}
}
