# Use an official Node.js runtime as the base image
FROM node:20-alpine AS base

# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine

# Install Docker CLI and dependencies
RUN apk add --no-cache \
    libc6-compat \
    docker-cli \
    docker-compose \
    curl \
    bash \
    shadow \
    sudo \
    uuidgen

# Set the working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json
COPY package*.json ./

# Make sure the prisma directory exists and copy schema file
COPY prisma ./prisma

# Install dependencies
RUN npm ci

# Copy the rest of the application code
COPY . .

# Copy the update script
COPY docker-update.sh /usr/local/bin/docker-update
RUN chmod +x /usr/local/bin/docker-update

# Build the Next.js application
RUN npm run build

# Create docker group and user with proper permissions
RUN addgroup -g 998 docker && \
    adduser -D -u 1001 app && \
    adduser app docker && \
    echo "app ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/app

# Production image, copy all the files and run next
FROM node:20-alpine AS runner

WORKDIR /app

# Install necessary tools in the final image
RUN apk add --no-cache \
    docker-cli \
    docker-compose \
    curl \
    bash \
    shadow \
    sudo \
    uuidgen

# Set up the same group and user
RUN addgroup -g 998 docker && \
    adduser -D -u 1001 app && \
    adduser app docker && \
    echo "app ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/app

# Copy files from build stage
COPY --from=base /app/node_modules ./node_modules
COPY --from=base /app/package.json ./package.json
COPY --from=base /app/dist ./dist
COPY --from=base /app/src ./src
# Copy the prisma directory to ensure schema.prisma is available
COPY --from=base /app/prisma ./prisma

# Copy the update script from build stage
COPY --from=base /usr/local/bin/docker-update /usr/local/bin/docker-update

# Create workspace directory and set proper permissions
RUN mkdir -p /workspace /app/data && \
    chown -R app:docker /app /workspace /app/data && \
    chmod 755 /usr/local/bin/docker-update && \
    chmod 777 /app/data

USER app

EXPOSE 3003

CMD ["npm", "run", "start"]