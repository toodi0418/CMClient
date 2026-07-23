# syntax=docker/dockerfile:1@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89

FROM node:22-bookworm-slim@sha256:6c74791e557ce11fc957704f6d4fe134a7bc8d6f5ca4403205b2966bd488f6b3 AS build

WORKDIR /workspace
ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0

RUN apt-get update \
    && apt-get install --yes --no-install-recommends python3 make g++ \
    && rm -rf /var/lib/apt/lists/* \
    && corepack enable \
    && corepack install --global pnpm@11.9.0

COPY . .

RUN pnpm install --frozen-lockfile \
    && pnpm --filter @cmclient/contracts run build \
    && pnpm --filter @cmclient/gateway run build \
    && pnpm --filter @cmclient/web run build \
    && pnpm --filter @cmclient/gateway deploy --prod --frozen-lockfile /opt/cmclient/gateway

FROM node:22-bookworm-slim@sha256:6c74791e557ce11fc957704f6d4fe134a7bc8d6f5ca4403205b2966bd488f6b3 AS runtime

ARG CMCLIENT_BUILD_VERSION=""
ARG CMCLIENT_BUILD_COMMIT=""
ARG CMCLIENT_BUILD_TREE=""
ARG CMCLIENT_BUILD_CHANNEL=""
ARG CMCLIENT_TARGET_ARCHITECTURE=""

RUN groupadd --gid 10001 cmclient \
    && useradd --uid 10001 --gid cmclient --home-dir /home/cmclient \
      --no-create-home --shell /usr/sbin/nologin cmclient \
    && mkdir --parents /home/cmclient/.cmclient \
    && touch /home/cmclient/.cmclient/.volume-initialized

WORKDIR /app
COPY --from=build --chown=cmclient:cmclient /opt/cmclient/gateway /app/gateway
COPY --from=build --chown=cmclient:cmclient /workspace/apps/web/dist /app/web
COPY --from=build --chown=cmclient:cmclient /workspace/proto /app/proto
COPY --from=build --chown=cmclient:cmclient /workspace/scripts/container-entrypoint.mjs /app/container-entrypoint.mjs
COPY --from=build --chown=cmclient:cmclient /workspace/scripts/container-runtime.mjs /app/container-runtime.mjs
RUN chown --recursive cmclient:cmclient /home/cmclient

ENV NODE_ENV=production \
    HOME=/home/cmclient \
    CMCLIENT_RUNTIME_ROOT=/home/cmclient/.cmclient \
    CMCLIENT_DB_PATH=/home/cmclient/.cmclient/cmclient.db \
    CMCLIENT_BACKUP_DIR=/home/cmclient/.cmclient/backups \
    CMCLIENT_BUILD_VERSION=${CMCLIENT_BUILD_VERSION} \
    CMCLIENT_BUILD_COMMIT=${CMCLIENT_BUILD_COMMIT} \
    CMCLIENT_BUILD_TREE=${CMCLIENT_BUILD_TREE} \
    CMCLIENT_BUILD_CHANNEL=${CMCLIENT_BUILD_CHANNEL} \
    CMCLIENT_RUNTIME_PROFILE=docker \
    CMCLIENT_PACKAGE_PROFILE=oci \
    CMCLIENT_TARGET_OS=linux \
    CMCLIENT_TARGET_ARCHITECTURE=${CMCLIENT_TARGET_ARCHITECTURE}

USER cmclient:cmclient

EXPOSE 8080

ENTRYPOINT ["node", "/app/container-entrypoint.mjs"]
CMD ["gateway"]
