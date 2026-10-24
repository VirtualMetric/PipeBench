# The link-fault helper. A scenario step runs it for a moment in the network
# namespace of the container whose traffic it changes:
#
#   docker run --rm --network container:<name> --cap-add NET_ADMIN \
#     vmetric/bench-nettool -c '<script>'
#
# and the script cuts, delays or drops that container's traffic to chosen
# peers with iptables and tc (netem). Nothing changes in the image under test.
#
# The harness builds this image rather than pulling a third-party one, so a tag
# that disappears from a registry cannot break the cases that use it. The base
# is pinned by digest; the two packages come from that Alpine release.
FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk add --no-cache iptables iproute2
ENTRYPOINT ["/bin/sh"]
