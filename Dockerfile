FROM debian:bookworm-slim AS builder
WORKDIR /powertagd/src

RUN apt-get update \
  && apt-get install --no-install-recommends --no-install-suggests -y \
    ca-certificates \
    build-essential

WORKDIR /powertagd/src

COPY src /powertagd/src

RUN set -xe \
  && make

FROM golang:1.24 AS bridge-build
WORKDIR /src

COPY go.mod go.sum /src/
RUN go mod download

COPY powertagd-bridge.go /src/
RUN go build -ldflags="-s -w" -o /src/powertagd-bridge /src/powertagd-bridge.go

FROM debian:bookworm-slim

LABEL org.opencontainers.image.source="https://github.com/omBratteng/powertagd"
LABEL org.opencontainers.image.url="https://github.com/omBratteng/powertagd"

COPY --from=builder /powertagd/src/powertagd /powertagd
COPY --from=bridge-build /src/powertagd-bridge /powertagd-bridge
COPY run.sh /run.sh

CMD ["/run.sh"]
