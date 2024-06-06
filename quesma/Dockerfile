FROM golang:alpine AS builder


WORKDIR /quesma

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

ENV GOCACHE=/root/.cache/go-build
ARG QUESMA_BUILD_SHA
ARG QUESMA_VERSION
ARG QUESMA_LICENSE_KEY
ARG QUESMA_BUILD_DATE

COPY util/healthcheck util/healthcheck

RUN --mount=type=cache,target="/root/.cache/go-build" \
    --mount=type=cache,target=/go/pkg/mod  \
    go build -o healthcheck ./util/healthcheck

COPY . .

RUN --mount=type=cache,target="/root/.cache/go-build" \
    --mount=type=cache,target=/go/pkg/mod  \
    go build \
    -ldflags=" \
    -X 'mitmproxy/quesma/buildinfo.BuildHash=$QUESMA_BUILD_SHA'  \
    -X 'buildinfo.BuildHash=$QUESMA_BUILD_SHA'  \
    -X 'mitmproxy/quesma/buildinfo.Version=$QUESMA_VERSION'  \
    -X 'buildinfo.Version=$QUESMA_VERSION'  \
    -X 'mitmproxy/quesma/buildinfo.LicenseKey=$QUESMA_LICENSE_KEY'  \
    -X 'buildinfo.LicenseKey=$QUESMA_LICENSE_KEY' \
    -X 'mitmproxy/quesma/buildinfo.BuildDate=$QUESMA_BUILD_DATE'  \
    -X 'buildinfo.BuildDate=$QUESMA_BUILD_DATE' \

    " \
    -o bin

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /quesma/bin /quesma/healthcheck ./

ENTRYPOINT [ "/bin" ]
HEALTHCHECK --interval=1s --timeout=1s --start-period=2s --retries=3 CMD [ "/healthcheck" ]
