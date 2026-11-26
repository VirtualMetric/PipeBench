# An SSH server for the cases that deploy an agent over SSH, such as the
# cluster cases with an agentless Linux device. OpenSSH is installed when the
# image is built, so a run downloads nothing. A case that installed it with
# apk when its endpoint started needed the package mirror from inside the
# run's network, and on some runners sshd never started.
#
# Root may sign in with a password. A case sets it in SSH_ROOT_PASSWORD, or
# with chpasswd in its own command. The host keys are made when the container
# starts, so no two containers share them.
#
# The harness builds this image rather than pulling a third-party one, so a tag
# that disappears from a registry cannot break the cases that use it. The base
# is pinned by digest, and the packages come from that Alpine release.
FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
# sed exits 0 when it matches nothing, so the build checks each setting after
# the edits. A newer base whose sshd_config words a setting otherwise fails
# here, not in the cases that sign in.
RUN apk add --no-cache openssh openssh-server openssh-sftp-server && \
    sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    (grep -q '^Subsystem sftp' /etc/ssh/sshd_config || echo 'Subsystem sftp /usr/lib/ssh/sftp-server' >> /etc/ssh/sshd_config) && \
    grep -q '^PermitRootLogin yes$' /etc/ssh/sshd_config && \
    grep -q '^PasswordAuthentication yes$' /etc/ssh/sshd_config && \
    grep -q '^Subsystem[[:space:]]\+sftp' /etc/ssh/sshd_config
COPY containers/sshhost/start.sh /start.sh
EXPOSE 22
ENTRYPOINT ["/bin/sh", "/start.sh"]
CMD ["/usr/sbin/sshd", "-D", "-e"]
