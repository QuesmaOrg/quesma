FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git

WORKDIR app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

ENV GOCACHE=/root/.cache/go-build

RUN --mount=type=cache,target="/root/.cache/go-build" go build -o bin
RUN --mount=type=cache,target="/root/.cache/go-build" go build -o healthcheck ./util/healthcheck

FROM scratch

COPY --from=builder /go/app/bin /go/app/healthcheck /go/app/config.yaml ./

ENTRYPOINT [ "/bin" ]
HEALTHCHECK --interval=1s --timeout=1s --start-period=2s --retries=3 CMD [ "/healthcheck" ]
