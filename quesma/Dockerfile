FROM golang:alpine AS builder


WORKDIR app

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

ENV GOCACHE=/root/.cache/go-build
ARG QUESMA_VERSION
ARG QUESMA_LICENSE_KEY

COPY util/healthcheck util/healthcheck

RUN --mount=type=cache,target="/root/.cache/go-build" \
    --mount=type=cache,target=/go/pkg/mod  \
    go build -o healthcheck ./util/healthcheck

COPY . .

RUN --mount=type=cache,target="/root/.cache/go-build" \
    --mount=type=cache,target=/go/pkg/mod  \
    go build \
    -ldflags="-X 'mitmproxy/quesma/buildinfo.Version=$QUESMA_VERSION'" \
    -ldflags="-X 'buildinfo.Version=$QUESMA_VERSION'" \
    -ldflags="-X 'mitmproxy/quesma/buildinfo.LicenseKey=$QUESMA_LICENSE_KEY'" \
    -ldflags="-X 'buildinfo.LicenseKey=$QUESMA_LICENSE_KEY'" \
    -o bin

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /go/app/bin /go/app/healthcheck /go/app/config.yaml ./

ENTRYPOINT [ "/bin" ]
HEALTHCHECK --interval=1s --timeout=1s --start-period=2s --retries=3 CMD [ "/healthcheck" ]
