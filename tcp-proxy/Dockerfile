# syntax=docker/dockerfile:1

FROM node:22-bookworm-slim

ENV NODE_ENV=production

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential python3 libudev-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY tcp-proxy ./tcp-proxy

USER node

EXPOSE 4403

CMD ["node", "tcp-proxy/index.js"]
