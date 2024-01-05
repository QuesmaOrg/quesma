FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git

ADD logger.go /logger.go

RUN go build -o /service /logger.go

FROM scratch

COPY --from=builder /service .

ENTRYPOINT [ "/service" ]
