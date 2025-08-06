# Этап сборки
FROM golang:1.23 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o linkreduction ./main.go

FROM debian:bullseye-slim

WORKDIR /app

COPY --from=builder /app/linkreduction .

COPY ./migrations ./migrations

COPY ./internal/config ./internal/config

RUN chmod +x ./linkreduction

EXPOSE 8080

CMD ["./linkreduction", "shorten"]