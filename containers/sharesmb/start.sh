#!/bin/sh
# Starts Samba with the share, and says it is ready in /run/share-ready once it
# listens. The password of the user bench is a constant of the harness, not a
# secret: the share holds only what a case writes.
set -e

# Docker mounts the share volume over SMB in the host's kernel
modprobe cifs 2>/dev/null || echo "start: cannot load the cifs module; the host must have it loaded" >&2

adduser -D -H bench 2>/dev/null || true
printf 'bench\nbench\n' | smbpasswd -a -s bench
mkdir -p /share
chmod 0777 /share

smbd --foreground --no-process-group --debug-stdout &
pid=$!
trap 'kill -TERM $pid; wait $pid' TERM INT
while ! nc -z 127.0.0.1 445; do
  sleep 0.2
done
touch /run/share-ready
wait $pid
