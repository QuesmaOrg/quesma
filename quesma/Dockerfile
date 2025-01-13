FROM golang:alpine AS builder


WORKDIR /quesma
## v2 is a seprate module, not package
COPY v2/go.mod v2/go.mod
COPY v2/go.sum v2/go.sum

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

ENV GOCACHE=/root/.cache/go-build
ARG QUESMA_BUILD_SHA
ARG QUESMA_VERSION
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
    -X 'github.com/QuesmaOrg/quesma/quesma/buildinfo.BuildHash=$QUESMA_BUILD_SHA'  \
    -X 'buildinfo.BuildHash=$QUESMA_BUILD_SHA'  \
    -X 'github.com/QuesmaOrg/quesma/quesma/buildinfo.Version=$QUESMA_VERSION'  \
    -X 'buildinfo.Version=$QUESMA_VERSION'  \
    -X 'github.com/QuesmaOrg/quesma/quesma/buildinfo.BuildDate=$QUESMA_BUILD_DATE'  \
    -X 'buildinfo.BuildDate=$QUESMA_BUILD_DATE' \
    " \
    -o bin

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /quesma/bin /quesma/healthcheck ./

ENTRYPOINT [ "/bin" ]
HEALTHCHECK --interval=1s --timeout=1s --start-period=2s --retries=3 CMD [ "/healthcheck" ]
