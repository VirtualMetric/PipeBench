#!/bin/sh
# Makes the container's host keys, sets root's password when SSH_ROOT_PASSWORD
# is set, and then runs the command: sshd in the foreground, unless the case
# gives a command of its own. A case that writes files for the agent to read
# runs its own shell, which ends with: exec /usr/sbin/sshd -D -e
set -e

ssh-keygen -A > /dev/null

if [ -n "${SSH_ROOT_PASSWORD:-}" ]; then
	echo "root:${SSH_ROOT_PASSWORD}" | chpasswd
fi

exec "$@"
