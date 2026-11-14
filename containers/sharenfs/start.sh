#!/bin/sh
# Starts the kernel's NFS server, version 4 only, with /export as its root,
# and says it is ready in /run/share-ready once its grace period is over.
#
# /export must be a volume: the kernel exports no overlay file system. The
# lease and the grace period are 10 seconds, not the kernel's 90. A new server
# opens no file during its grace period, so a case would wait 90 seconds for
# its first write. The lease is also how long the server keeps the state of a
# client that it lost, such as a replica that a case killed.
set -e

modprobe nfsd 2>/dev/null || echo "start: cannot load the nfsd module; the host must have it loaded" >&2
# Docker mounts the share volume over NFS in the host's kernel
modprobe nfs 2>/dev/null || echo "start: cannot load the nfs module; the host must have it loaded" >&2

mkdir -p /export
chmod 0777 /export
echo "/export *(rw,sync,no_subtree_check,no_root_squash,insecure,fsid=0)" > /etc/exports
mountpoint -q /proc/fs/nfsd || mount -t nfsd nfsd /proc/fs/nfsd
echo 10 > /proc/fs/nfsd/nfsv4leasetime
echo 10 > /proc/fs/nfsd/nfsv4gracetime

stop() {
  rpc.nfsd 0 || true
  exportfs -ua || true
  exit 0
}
trap stop TERM INT

rpc.nfsd -N 3 -V 4 8
exportfs -ra
rpc.mountd -N 3 -V 4 --foreground &
sleep 11
touch /run/share-ready
wait
