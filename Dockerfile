# Пин до patch-версии, не rolling: официальный образ ставит GOTOOLCHAIN=local,
# поэтому образ СТАРШЕ go.mod не подтягивает нужный тулчейн, а падает на
# `go mod download` — ровно так этот Dockerfile и не собирался ни разу
# (golang:1.24 против `go 1.25` в go.mod).
FROM golang:1.27.0-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X github.com/tatnet-ru/tatnet-csi-driver/internal/driver.Version=${VERSION}" \
      -o /out/tatnet-csi-driver ./cmd/tatnet-csi-driver

# Не distroless: node-плагину нужны mkfs.ext4/xfs и resize2fs/xfs_growfs —
# k8s.io/mount-utils вызывает их как внешние программы. Образ без них
# поднимается и падает на первом же PVC, причём в момент монтирования, а не при
# старте.
FROM debian:bookworm-20260803-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends e2fsprogs xfsprogs util-linux ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/tatnet-csi-driver /usr/local/bin/tatnet-csi-driver
ENTRYPOINT ["/usr/local/bin/tatnet-csi-driver"]
