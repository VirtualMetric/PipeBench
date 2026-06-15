package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/VirtualMetric/PipeBench/internal/config"
)

// Kerberos topology constants. All values are local, throwaway, and never real:
// the realm/keytabs live in the per-run temp dir and are removed with it.
const (
	// kerberosKDCPassword is the KDB master password used by kdb5_util.
	kerberosKDCPassword = "pipebench-kdc-dev"
	// kerberosBrokerHost is the SPN host half (service/host@REALM). It MUST
	// equal the broker's advertised hostname on the bench network, or GSSAPI
	// fails with "server not found in Kerberos database".
	kerberosBrokerHost = "kafka"
	// kerberosClientPrincipal is the principal the subject (and any client)
	// authenticates as via /krb5/client.keytab.
	kerberosClientPrincipal = "bench"
)

// KerberosPaths is what PrepareKerberos provisions for a gssapi (`kafka.auth:
// {mechanism: gssapi}`) case.
type KerberosPaths struct {
	// Dir is the host krb5 directory, bind-mounted read-write into the KDC
	// (which creates the realm DB and writes the keytabs there at boot) and
	// read-only into the broker and subject (which read the keytabs + krb5.conf).
	Dir string
	// InitCmd is the KDC bootstrap shell line rendered into the kdc service
	// command: create the realm DB, add the broker SPN + client principal,
	// export their keytabs, signal readiness, then run krb5kdc in the
	// foreground. Realm/service are charset-validated at case load, so they are
	// safe to embed (mirrors the vault-init command).
	InitCmd string
}

// PrepareKerberos creates the host krb5 dir and renders krb5.conf + kdc.conf for
// a gssapi case, and builds the KDC bootstrap command. The keytabs are produced
// by the KDC container at boot (kadmin must run inside the krb5 image), so the
// broker and subject mount Dir read-only and read the keytabs from there.
func PrepareKerberos(tmpDir string, k *config.KafkaConfig) (KerberosPaths, error) {
	dir := filepath.Join(tmpDir, "krb5")
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return KerberosPaths{}, fmt.Errorf("creating krb5 dir: %w", err)
	}
	// World-writable: the KDC image may run as a non-root user and must create
	// the realm DB + keytabs in this host-owned dir. Per-run throwaway, removed
	// with the temp dir (mirrors the vault TLS dir).
	if err := os.Chmod(dir, 0o777); err != nil {
		return KerberosPaths{}, fmt.Errorf("chmod krb5 dir: %w", err)
	}

	realm := k.RealmOrDefault()
	service := k.ServiceNameOrDefault()

	// krb5.conf: one realm, KDC + admin server at "kdc". udp_preference_limit
	// = 1 forces TCP (matches the backend's generated config) and avoids UDP
	// fragmentation/retry with MIT/AD KDCs.
	krb5Conf := fmt.Sprintf(`[libdefaults]
    default_realm = %[1]s
    dns_lookup_realm = false
    dns_lookup_kdc = false
    udp_preference_limit = 1
    rdns = false

[realms]
    %[1]s = {
        kdc = kdc:88
        admin_server = kdc
    }
`, realm)
	if err := os.WriteFile(filepath.Join(dir, "krb5.conf"), []byte(krb5Conf), 0o644); err != nil {
		return KerberosPaths{}, fmt.Errorf("writing krb5.conf: %w", err)
	}

	// kdc.conf: realm DB + key material live under /krb5 (the writable mount),
	// so the per-run KDB is thrown away with the temp dir. aes-sha1 enctypes are
	// accepted by gokrb5 and the JVM with no extra JCE.
	kdcConf := fmt.Sprintf(`[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    %[1]s = {
        database_name = /krb5/principal
        key_stash_file = /krb5/.k5.%[1]s
        acl_file = /krb5/kadm5.acl
        supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
    }
`, realm)
	if err := os.WriteFile(filepath.Join(dir, "kdc.conf"), []byte(kdcConf), 0o644); err != nil {
		return KerberosPaths{}, fmt.Errorf("writing kdc.conf: %w", err)
	}

	brokerSPN := fmt.Sprintf("%s/%s@%s", service, kerberosBrokerHost, realm)
	clientPN := fmt.Sprintf("%s@%s", kerberosClientPrincipal, realm)

	// Bootstrap: point krb5 at the mounted configs, create the KDB, add the
	// broker SPN + client principal with random keys, export world-readable
	// keytabs (the non-root broker/subject must read them — same lesson as the
	// TLS keys), signal readiness, then run the KDC in the foreground.
	var sb strings.Builder
	sb.WriteString("set -e; ")
	sb.WriteString("export KRB5_CONFIG=/krb5/krb5.conf KRB5_KDC_PROFILE=/krb5/kdc.conf; ")
	fmt.Fprintf(&sb, "kdb5_util create -s -r %s -P %s; ", realm, kerberosKDCPassword)
	fmt.Fprintf(&sb, "kadmin.local -q 'addprinc -randkey %s'; ", brokerSPN)
	fmt.Fprintf(&sb, "kadmin.local -q 'addprinc -randkey %s'; ", clientPN)
	fmt.Fprintf(&sb, "kadmin.local -q 'ktadd -k /krb5/broker.keytab %s'; ", brokerSPN)
	fmt.Fprintf(&sb, "kadmin.local -q 'ktadd -k /krb5/client.keytab %s'; ", clientPN)
	sb.WriteString("chmod 0644 /krb5/broker.keytab /krb5/client.keytab; ")
	// .ready signals the broker (which only needs its keytab on disk at boot)
	// that the keytabs exist; clients reach the live KDC later, at auth time.
	sb.WriteString("touch /krb5/.ready; ")
	sb.WriteString("exec krb5kdc -n")

	return KerberosPaths{Dir: dir, InitCmd: sb.String()}, nil
}
