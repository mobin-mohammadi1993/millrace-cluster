FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/millrace-cluster ./cmd/millrace-cluster

FROM alpine:3.20
COPY --from=build /out/millrace-cluster /usr/local/bin/millrace-cluster
ENTRYPOINT ["/usr/local/bin/millrace-cluster"]
