# Control plane: TypeScript API run with tsx (no build step needed for v0).
FROM node:22-alpine

WORKDIR /app

# Install dependencies first for better layer caching.
COPY package.json package-lock.json ./
RUN npm ci

# App source + migration assets.
COPY tsconfig.json drizzle.config.ts ./
COPY src ./src
COPY drizzle ./drizzle

EXPOSE 3000

CMD ["npx", "tsx", "src/index.ts"]
