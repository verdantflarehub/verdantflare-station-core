# syntax=docker/dockerfile:1
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/station-core ./cmd/station-core

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/station-core /station-core
ENV STATION_LISTEN_ADDR=0.0.0.0:5050
EXPOSE 5050
USER 65532:65532
ENTRYPOINT ["/station-core"]
CMD ["serve"]
