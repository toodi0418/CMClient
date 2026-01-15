# syntax=docker/dockerfile:1

FROM node:22-bookworm-slim AS base

ENV NODE_ENV=production \
    TMAG_WEB_PORT=7080 \
    CALLMESH_ARTIFACTS_DIR=/data/callmesh \
    CALLMESH_VERIFICATION_FILE=/data/callmesh/monitor.json

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY . .

RUN mkdir -p /data/callmesh \
    && chown -R node:node /app /data

USER node

EXPOSE 7080
VOLUME ["/data/callmesh"]

CMD ["npm", "start"]
