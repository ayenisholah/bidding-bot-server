# Use an official Node.js runtime as the base image
FROM node:20-alpine AS base

# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine

# Install dependencies
RUN apk add --no-cache \
    libc6-compat \
    curl \
    bash

# Set the working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json
COPY package*.json ./


# Install dependencies
RUN npm ci

# Copy the rest of the application code
COPY . .



# Build the Next.js application
RUN npm run build

# Create user
RUN adduser -D -u 1001 app

# Production image, copy all the files and run next
FROM node:20-alpine AS runner

WORKDIR /app

# Install necessary tools in the final image
RUN apk add --no-cache \
    curl \
    bash

# Set up user
RUN adduser -D -u 1001 app

# Copy files from build stage
COPY --from=base /app/node_modules ./node_modules
COPY --from=base /app/package.json ./package.json
COPY --from=base /app/dist ./dist
COPY --from=base /app/src ./src


# Create data and debug-logs directories and set proper permissions
RUN mkdir -p /app/data /app/debug-logs && \
    chown -R app:app /app /app/data /app/debug-logs && \
    chmod 755 /app/data /app/debug-logs

USER app

EXPOSE 3003

CMD ["npm", "run", "start"]