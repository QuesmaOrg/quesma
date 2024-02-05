FROM golang:alpine AS builder

ADD querygenerator.go /querygenerator.go

RUN go build -o /service /querygenerator.go

FROM scratch

COPY --from=builder /service .

ENTRYPOINT [ "/service" ]
