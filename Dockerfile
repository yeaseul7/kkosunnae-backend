FROM node:24-bookworm-slim AS builder

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    g++ \
    make \
    python3 \
  && rm -rf /var/lib/apt/lists/*

COPY functions/package*.json ./
RUN npm ci

COPY functions/tsconfig*.json ./
COPY functions/src ./src

RUN npm run build

# Preload the image embedding model so Cloud Run does not download it on cold start.
RUN node --input-type=module -e "import {pipeline} from '@huggingface/transformers'; await pipeline('image-feature-extraction', 'Xenova/dinov2-small');"

RUN npm prune --omit=dev

FROM node:24-bookworm-slim

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=8080

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgomp1 \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder --chown=node:node /app/package*.json ./
COPY --from=builder --chown=node:node /app/node_modules ./node_modules
COPY --from=builder --chown=node:node /app/lib ./lib

USER node

EXPOSE 8080

CMD ["npm", "start"]
