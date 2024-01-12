FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git

ADD querygenerator.go /querygenerator.go

RUN go build -o /service /querygenerator.go

FROM scratch

COPY --from=builder /service .

ENTRYPOINT [ "/service" ]
