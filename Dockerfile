# syntax=docker/dockerfile:1

FROM node:22-bookworm-slim AS build

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

FROM node:22-bookworm-slim AS runtime

RUN groupadd --gid 10001 cmclient \
    && useradd --uid 10001 --gid cmclient --home-dir /nonexistent \
      --no-create-home --shell /usr/sbin/nologin cmclient \
    && mkdir --parents /var/lib/cmclient \
    && touch /var/lib/cmclient/.volume-initialized

WORKDIR /app
COPY --from=build --chown=cmclient:cmclient /opt/cmclient/gateway /app/gateway
COPY --from=build --chown=cmclient:cmclient /workspace/apps/web/dist /app/web
COPY --from=build --chown=cmclient:cmclient /workspace/proto /app/proto
COPY --from=build --chown=cmclient:cmclient /workspace/scripts/container-entrypoint.mjs /app/container-entrypoint.mjs
COPY --from=build --chown=cmclient:cmclient /workspace/scripts/container-runtime.mjs /app/container-runtime.mjs
RUN chown --recursive cmclient:cmclient /var/lib/cmclient

ENV NODE_ENV=production \
    CMCLIENT_DATA_DIR=/var/lib/cmclient

USER cmclient:cmclient

EXPOSE 8080

ENTRYPOINT ["node", "/app/container-entrypoint.mjs"]
CMD ["gateway"]
