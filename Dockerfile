# First stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

COPY vendor ./vendor

COPY . .

# Setting CGO_ENABLED=0 bundles all the libraries statically when building the executable
RUN GOOS=linux CGO_ENABLED=0 go build -mod vendor -ldflags="-w -s" -o kvserver ./cmd/kvserver

# Production stage
FROM gcr.io/distroless/static-debian12:latest

COPY --from=builder /app/kvserver /

EXPOSE 6379

ENTRYPOINT ["/kvserver"]