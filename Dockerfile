# syntax=docker/dockerfile:1
ARG VERSION=dev

ARG BUILD_REPO=golang
ARG BUILD_TAG=1.24-bookworm
ARG BUILD_PACKAGES="git g++ cmake ninja-build libssl-dev ca-certificates procps net-tools"

ARG IMAGE_REPO=debian
ARG IMAGE_TAG=bookworm-slim
ARG IMAGE_PACKAGES=ca-certificates

ARG UPDATE_PACKAGES="apt update"
ARG ADD_PACKAGES="apt install -y"

FROM --platform=$BUILDPLATFORM $BUILD_REPO:$BUILD_TAG AS build
ARG ADD_PACKAGES BUILD_PACKAGES UPDATE_PACKAGES

WORKDIR /build
RUN $UPDATE_PACKAGES
RUN $ADD_PACKAGES $BUILD_PACKAGES
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg go mod download
RUN go install github.com/go-delve/delve/cmd/dlv@latest
COPY . ./

FROM build AS build-lakefs
ARG VERSION TARGETOS TARGETARCH ADD_PACKAGES BUILD_PACKAGES UPDATE_PACKAGES
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -gcflags "all=-N -l" -ldflags "-X github.com/treeverse/lakefs/pkg/version.Version=${VERSION}" -o lakefs ./cmd/lakefs

FROM build AS build-lakectl
ARG VERSION TARGETOS TARGETARCH ADD_PACKAGES BUILD_PACKAGES UPDATE_PACKAGES
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags "-X github.com/treeverse/lakefs/pkg/version.Version=${VERSION}" -o lakectl ./cmd/lakectl

FROM $IMAGE_REPO:$IMAGE_TAG AS lakectl
ARG ADD_PACKAGES IMAGE_PACKAGES UPDATE_PACKAGES
WORKDIR /app
ENV PATH=/app:$PATH
COPY --from=build-lakectl /build/lakectl /app/
COPY --from=build-lakectl /go/bin/dlv /usr/local/bin/
RUN $UPDATE_PACKAGES
RUN $ADD_PACKAGES $IMAGE_PACKAGES
RUN addgroup --system lakefs && adduser --system --ingroup lakefs lakefs
USER lakefs
WORKDIR /home/lakefs
ENTRYPOINT ["/app/lakectl"]

FROM lakectl AS lakefs
LABEL org.opencontainers.image.source=https://github.com/invergent-ai/surogate-hub
COPY ./scripts/wait-for /app/
COPY --from=build-lakefs /build/lakefs /app/
EXPOSE 8000/tcp
EXPOSE 2345/tcp
# ENTRYPOINT ["/usr/local/bin/dlv", "--listen=:2345", "--headless=true", "--api-version=2", "--accept-multiclient", "exec", "--", "/app/lakefs"]
ENTRYPOINT ["/app/lakefs"]
CMD ["run"]
