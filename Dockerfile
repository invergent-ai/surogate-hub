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
COPY . ./

FROM build AS build-lakefs
ARG VERSION TARGETOS TARGETARCH ADD_PACKAGES BUILD_PACKAGES UPDATE_PACKAGES
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -gcflags "all=-N -l" -ldflags "-X github.com/treeverse/lakefs/pkg/version.Version=${VERSION}" -o lakefs ./cmd/lakefs

# Build the stats worker virtualenv on the same base as the final image so
# the Python binary in the venv is ABI-compatible at runtime.
FROM $IMAGE_REPO:$IMAGE_TAG AS build-stats-worker
ARG ADD_PACKAGES IMAGE_PACKAGES UPDATE_PACKAGES
RUN $UPDATE_PACKAGES \
    && $ADD_PACKAGES ca-certificates python3 python3-venv python3-pip
WORKDIR /src
COPY clients/python /src/clients/python
COPY stats-worker /src/stats-worker
RUN python3 -m venv /opt/stats-worker-venv \
    && /opt/stats-worker-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/stats-worker-venv/bin/pip install --no-cache-dir \
        /src/clients/python \
        /src/stats-worker

FROM $IMAGE_REPO:$IMAGE_TAG AS lakefs
ARG ADD_PACKAGES IMAGE_PACKAGES UPDATE_PACKAGES
LABEL org.opencontainers.image.source=https://github.com/invergent-ai/surogate-hub
WORKDIR /app
ENV PATH=/app:$PATH
RUN $UPDATE_PACKAGES \
    && $ADD_PACKAGES $IMAGE_PACKAGES python3 \
    && rm -rf /var/lib/apt/lists/*
RUN addgroup --system lakefs && adduser --system --ingroup lakefs lakefs
COPY --from=build-lakefs /build/lakefs /app/
COPY ./scripts/wait-for /app/
COPY --from=build-stats-worker /opt/stats-worker-venv /opt/stats-worker-venv
RUN printf '#!/bin/sh\nexec /opt/stats-worker-venv/bin/python -m surogate_hub_worker "$@"\n' \
        > /app/stats-worker \
    && chmod +x /app/stats-worker
USER lakefs
WORKDIR /home/lakefs
EXPOSE 8000/tcp
ENTRYPOINT ["/app/lakefs"]
CMD ["run"]
