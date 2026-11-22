FROM oven/bun:1.4.2-alpine@sha256:d888c0ae6c86d7866ff10c5aafdd9077b36aee6455b33dd270fb93c0dd5cef6f AS ui-build
ARG VERSION=dev
WORKDIR /src
COPY openapi ./openapi
COPY ui/package.json ui/bun.lock ./ui/
RUN bun install --frozen-lockfile --cwd ui
COPY ui ./ui
RUN VERSION="${VERSION}" bun run --cwd ui build

FROM golang:1.27.4-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS build
WORKDIR /src
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
ARG SNAPSHOT=unknown
ARG DIRTY=unknown
ARG SOURCE_DATE_EPOCH=0
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=ui-build /src/ui/dist ./ui/dist
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${BUILD_DATE} -X main.snapshot=${SNAPSHOT} -X main.dirty=${DIRTY}" \
    -o /out/teldrive ./cmd/teldrive
RUN touch -d "@${SOURCE_DATE_EPOCH}" /out/teldrive

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG COMMIT=unknown
ARG VERSION=dev
ARG SNAPSHOT=unknown
ARG DIRTY=unknown
ARG DISTROLESS_DIGEST=unknown
LABEL org.opencontainers.image.revision=$COMMIT \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.base.name="gcr.io/distroless/static-debian12:nonroot" \
      org.opencontainers.image.base.digest=$DISTROLESS_DIGEST \
      io.enchanced.source.snapshot=$SNAPSHOT \
      io.enchanced.source.dirty=$DIRTY
COPY --from=build /out/teldrive /usr/bin/teldrive
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/bin/teldrive"]
CMD ["serve"]
