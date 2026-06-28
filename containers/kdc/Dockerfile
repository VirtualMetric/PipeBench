# Minimal MIT Kerberos KDC for the kafka gssapi correctness case. Alpine-based
# (same base as the other bench helpers — smaller CVE surface than debian). Ships
# only the krb5 server tooling (kdb5_util, kadmin.local, krb5kdc in /usr/sbin);
# the realm DB, principals, and keytabs are created at runtime by the bootstrap
# command the harness renders (see internal/orchestrator/kerberos.go) into the
# mounted /krb5 dir (chmod 0777 there so this non-root user can write to it).
FROM alpine:3.22.5@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce
RUN apk upgrade --no-cache && apk add --no-cache krb5-server && adduser -D -u 10001 app

# Run non-root (uid matches the other helpers). krb5kdc binds privileged port 88
# fine because Docker sets net.ipv4.ip_unprivileged_port_start=0 by default, so
# no root and no file capability are needed. Do NOT setcap krb5kdc to grant the
# bind: a file capability marks the process AT_SECURE, and MIT krb5 then ignores
# KRB5_CONFIG/KRB5_KDC_PROFILE, so it loses the mounted /krb5 config + DB and
# dies with "cannot initialize realm". Plain non-root is both simpler and works.
USER app

# The harness drives this via `entrypoint: ["/bin/sh","-c"]` + a bootstrap
# command, exactly like the redpanda-init / vault-init services.
ENTRYPOINT ["/bin/sh", "-c"]
