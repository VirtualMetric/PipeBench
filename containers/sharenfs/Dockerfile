# The NFS server of the shared-state cases that keep their share on NFS
# (shared_state.store share with share_server nfs): the kernel's NFS server,
# version 4 only. The export is "sync": the server writes a file to its disk
# before it answers a client's COMMIT, as Azure Files' NFS shares do.
#
# The kernel's NFS server runs in the host's kernel, so the container is
# privileged. It loads the nfsd module, and the nfs module that Docker needs to
# mount the case's share volume, from the host's /lib/modules.
#
# The harness builds this image rather than pulling a third-party one, so a tag
# that disappears from a registry cannot break the cases that use it. The base
# is pinned by digest, and the packages come from that Alpine release.
FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk add --no-cache nfs-utils kmod
COPY containers/sharenfs/start.sh /start.sh
EXPOSE 2049
ENTRYPOINT ["/bin/sh", "/start.sh"]
