FROM alpine:3.21 AS base
RUN apk add --no-cache ca-certificates

# Copy the pre-built binary into the image.
# Build context should contain the vmetric-director binary.
COPY director /director
RUN chmod +x /director

# Debug config — placed next to the binary so director finds it
COPY vmetric.yml /vmetric.yml

# Config directory — mounted at runtime by the harness
RUN mkdir -p /etc/vmetric

WORKDIR /

ENTRYPOINT ["/director"]
