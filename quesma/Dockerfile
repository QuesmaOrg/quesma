FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git

ADD quesma.go /app.go

RUN go build -o /service /app.go

FROM scratch

COPY --from=builder /service .

ENTRYPOINT [ "/service" ]
