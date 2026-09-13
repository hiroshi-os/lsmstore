FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN go test ./... && CGO_ENABLED=0 go build -o /out/lsmstore ./cmd/lsmstore

FROM alpine:3.20
RUN adduser -D -H lsm
USER lsm
WORKDIR /data
COPY --from=build /out/lsmstore /usr/local/bin/lsmstore
EXPOSE 8080
ENTRYPOINT ["lsmstore"]
CMD ["-addr", ":8080", "-dir", "/data"]
