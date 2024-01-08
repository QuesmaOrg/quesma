FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git

COPY . app/

WORKDIR app
RUN go mod download
RUN go build -o /service

FROM scratch

COPY --from=builder /service .

ENTRYPOINT [ "/service" ]
