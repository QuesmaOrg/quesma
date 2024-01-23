FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git

COPY . app/


WORKDIR app
RUN go mod download
RUN go build -o bin

FROM scratch
COPY --from=builder /go/app/bin .
COPY --from=builder /go/app/config.yaml .


ENTRYPOINT [ "/bin" ]
