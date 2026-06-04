# syntax = docker/dockerfile:1.4

ARG BASE_IMAGE=ghcr.io/dbca-wa/docker-apps-dev:ubuntu_2604_base_python

# --- Builder: all build-time tools, Node.js, Python venv, Vue build, collectstatic ---
FROM ${BASE_IMAGE} AS builder

LABEL maintainer="asi@dbca.wa.gov.au"
LABEL org.opencontainers.image.source="https://github.com/dbca-wa/boranga"

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Australia/Perth \
    NODE_MAJOR=24 \
    NODE_OPTIONS=--max_old_space_size=4096 \
    PROJ_NETWORK=ON \
    SECRET_KEY="ThisisNotRealKey" \
    PRODUCTION_EMAIL=True \
    SITE_PREFIX='qml-dev' \
    SITE_DOMAIN='dbca.wa.gov.au' \
    BASE_URL='https://qml-dev.dbca.wa.gov.au' \
    NOMOS_BLOB_URL='https://nomos.example.com'

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install --no-install-recommends -y \
    bzip2 \
    ca-certificates \
    curl \
    g++ \
    libgdal-dev \
    python3-venv \
    software-properties-common \
    git \
    zlib1g-dev \
    libbz2-dev \
    build-essential && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Node.js and clean up in the same layer.
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" \
    | tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && \
    apt-get install --no-install-recommends -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -g 5000 oim && \
    useradd -g 5000 -u 5000 oim -s /bin/bash -d /app && \
    mkdir /app && \
    chown -R oim:oim /app

WORKDIR /app
USER oim

ENV VIRTUAL_ENV=/app/venv
ENV PATH=$VIRTUAL_ENV/bin:$PATH

# 1) Copy only requirements first so pip install is cached independently of code changes.
COPY --chown=oim:oim requirements.txt ./
RUN python3 -m venv $VIRTUAL_ENV && \
    $VIRTUAL_ENV/bin/pip install --upgrade pip && \
    $VIRTUAL_ENV/bin/pip install --no-cache-dir -r requirements.txt

# 2) Copy application code (changes here won't bust the pip cache).
COPY --chown=oim:oim gunicorn.ini.py manage.py python-cron ./
COPY --chown=oim:oim boranga ./boranga
COPY --chown=oim:oim scripts/combine_csvs.py \
                      scripts/dedupe_errors.py \
                      scripts/generate_uat_fixtures.sh \
                      scripts/partition_migration_data.py \
                      scripts/split_csv.py \
                      ./scripts/

# Build Vue frontend, then discard node_modules so they aren't copied to runtime.
RUN cd /app/boranga/frontend/boranga && npm ci --omit=dev
RUN cd /app/boranga/frontend/boranga && npm run build && \
    rm -rf /app/boranga/frontend/boranga/node_modules

RUN touch /app/.env && \
    $VIRTUAL_ENV/bin/python manage.py collectstatic --noinput

# --- Runtime: clean image with only runtime packages and built artifacts ---
FROM ${BASE_IMAGE} AS runtime

LABEL maintainer="asi@dbca.wa.gov.au"
LABEL org.opencontainers.image.source="https://github.com/dbca-wa/boranga"

ENV DEBIAN_FRONTEND=noninteractive \
    DEBUG=True \
    TZ=Australia/Perth \
    PRODUCTION_EMAIL=True \
    SECRET_KEY="ThisisNotRealKey" \
    SITE_PREFIX='qml-dev' \
    SITE_DOMAIN='dbca.wa.gov.au' \
    OSCAR_SHOP_NAME='Parks & Wildlife' \
    BPAY_ALLOWED=False \
    PROJ_NETWORK=ON

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install --no-install-recommends -y \
    ca-certificates \
    tzdata \
    wget && \
    apt-get remove --purge -y binutils rust-coreutils git mtr patch vim 2>/dev/null || true && \
    apt-get autoremove -y && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -g 5000 oim && \
    useradd -g 5000 -u 5000 oim -s /bin/bash -d /app && \
    mkdir -p /app/logs && \
    chown -R oim:oim /app && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY startup.sh /
RUN chmod 755 /startup.sh

USER oim
WORKDIR /app

ENV VIRTUAL_ENV=/app/venv
ENV PATH=$VIRTUAL_ENV/bin:$PATH

COPY --from=builder --chown=oim:oim /app/venv /app/venv
COPY --from=builder --chown=oim:oim /app/boranga /app/boranga
COPY --from=builder --chown=oim:oim /app/staticfiles /app/staticfiles
COPY --from=builder --chown=oim:oim /app/gunicorn.ini.py /app/gunicorn.ini.py
COPY --from=builder --chown=oim:oim /app/manage.py /app/manage.py
COPY --from=builder --chown=oim:oim /app/.env /app/.env
COPY --from=builder --chown=oim:oim /app/python-cron /app/python-cron
COPY --from=builder --chown=oim:oim /app/scripts /app/scripts

EXPOSE 8080
HEALTHCHECK --interval=1m --timeout=5s --start-period=10s --retries=3 CMD ["wget", "-q", "-O", "-", "http://localhost:8080/"]
CMD ["/startup.sh"]
