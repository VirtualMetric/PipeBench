# Minimal MIT Kerberos KDC for the kafka gssapi correctness case. It ships only
# the krb5 tooling (kdb5_util, kadmin.local, krb5kdc); the realm DB, principals,
# and keytabs are created at runtime by the bootstrap command the harness
# renders (see internal/orchestrator/kerberos.go) into the mounted /krb5 dir.
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        krb5-kdc krb5-admin-server krb5-user \
    && rm -rf /var/lib/apt/lists/*

# The harness drives this via `entrypoint: ["/bin/sh","-c"]` + a bootstrap
# command, exactly like the redpanda-init / vault-init services.
ENTRYPOINT ["/bin/sh", "-c"]
