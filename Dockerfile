# syntax=docker/dockerfile:1

# ---- Base Stage ----
FROM node:22-alpine AS base
WORKDIR /app
RUN npm install -g bun

# ---- Dependencies Stage ----
FROM base AS deps
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# ---- Builder Stage ----
FROM base AS builder
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN bun run build
# Prune development dependencies for final production image
RUN rm -rf node_modules && bun install --production --frozen-lockfile

# ---- Production Runner Stage ----
FROM node:22-alpine AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3000

# Run as non-root user for security
USER node

COPY --chown=node:node package.json ./
COPY --chown=node:node --from=builder /app/node_modules ./node_modules
COPY --chown=node:node --from=builder /app/dist ./dist

EXPOSE 3000

CMD ["node", "dist/main.js"]
