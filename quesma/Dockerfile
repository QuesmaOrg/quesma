FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git

COPY go.mod go.sum ./app/
COPY quesma.go /app/app.go

WORKDIR app
RUN go mod download
RUN go build -o /service /app/app.go

FROM scratch

COPY --from=builder /service .

ENTRYPOINT [ "/service" ]
