FROM node:22-alpine

WORKDIR /app
ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=8080 \
    METRICS_PORT=9090 \
    NODE_OPTIONS=--max-old-space-size=256 \
    REMOTE_BIN=ssh \
    REMOTE_PORT=22 \
    REMOTE_KEY_PATH=/run/secrets/id_ed25519 \
    REMOTE_USER=execmcp \
    REMOTE_STRICT_HOST_KEY_CHECKING=yes \
    REMOTE_KNOWN_HOSTS_PATH=/run/secrets/known_hosts \
    ALLOWED_CWDS=/workspace,/tmp \
    DEFAULT_CWD=/workspace
RUN apk add --no-cache openssh-client
COPY package.json package-lock.json README.md DESIGN.md LICENSE ./
COPY src ./src
COPY scripts ./scripts
RUN addgroup -S execmcp && adduser -S -G execmcp execmcp && mkdir -p /workspace && chmod +x scripts/*.sh
EXPOSE 8080 9090
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 CMD node -e "fetch('http://127.0.0.1:8080/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["node", "src/server.js"]
CMD []
