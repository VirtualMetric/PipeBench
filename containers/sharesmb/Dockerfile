# The SMB server of the shared-state cases that keep their share on SMB
# (shared_state.store share with share_server smb). Samba serves one share,
# "share", to the user bench, over SMB 3. "strict sync" makes it flush a file
# to its disk when a client flushes the file, as Azure Files does.
#
# Docker mounts the case's share volume over SMB in the host's kernel, which
# needs the kernel's cifs module. The container loads it when it starts, from
# the host's /lib/modules, and runs privileged for that alone.
#
# The harness builds this image rather than pulling a third-party one, so a tag
# that disappears from a registry cannot break the cases that use it. The base
# is pinned by digest, and the packages come from that Alpine release.
FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk add --no-cache samba kmod
COPY containers/sharesmb/smb.conf /etc/samba/smb.conf
COPY containers/sharesmb/start.sh /start.sh
EXPOSE 445
ENTRYPOINT ["/bin/sh", "/start.sh"]
