FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/quorum-server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/quorum-worker ./cmd/worker

FROM alpine:latest

RUN apk add --no-cache ca-certificates bash curl

WORKDIR /app

COPY --from=builder /app/bin/quorum-server /app/quorum-server
COPY --from=builder /app/bin/quorum-worker /app/quorum-worker

EXPOSE 8080 50051 18088

CMD ["/app/quorum-server"]
