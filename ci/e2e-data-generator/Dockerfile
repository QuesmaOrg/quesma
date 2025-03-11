FROM golang:alpine AS builder

ADD . /go/

RUN --mount=type=cache,target="/root/.cache/go-build" \
    --mount=type=cache,target=/go/pkg/mod  \
     go build -o /service .

FROM scratch
COPY --from=builder /service .
ENTRYPOINT [ "/service" ]
