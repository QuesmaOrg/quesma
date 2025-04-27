FROM golang:alpine AS builder

COPY platform /platform
COPY cmd /cmd

ENV GOOS=linux
ENV GOARCH=amd64
ENV GOCACHE=/root/.cache/go-build
ARG QUESMA_BUILD_SHA
ARG QUESMA_VERSION
ARG QUESMA_BUILD_DATE

WORKDIR /cmd

RUN --mount=type=cache,target="/root/.cache/go-build" \
    --mount=type=cache,target=/go/pkg/mod  \
    go build -o healthcheck ./util/healthcheck


RUN --mount=type=cache,target="/root/.cache/go-build" \
    --mount=type=cache,target=/go/pkg/mod  \
    go build \
    -ldflags=" \
    -X 'github.com/QuesmaOrg/quesma/platform/buildinfo.BuildHash=$QUESMA_BUILD_SHA'  \
    -X 'buildinfo.BuildHash=$QUESMA_BUILD_SHA'  \
    -X 'github.com/QuesmaOrg/quesma/platform/buildinfo.Version=$QUESMA_VERSION'  \
    -X 'buildinfo.Version=$QUESMA_VERSION'  \
    -X 'github.com/QuesmaOrg/quesma/platform/buildinfo.BuildDate=$QUESMA_BUILD_DATE'  \
    -X 'buildinfo.BuildDate=$QUESMA_BUILD_DATE' \
    " \
    -o bin

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /cmd/bin /cmd/healthcheck ./

ENTRYPOINT [ "/bin" ]
HEALTHCHECK --interval=1s --timeout=1s --start-period=2s --retries=3 CMD [ "/healthcheck" ]
