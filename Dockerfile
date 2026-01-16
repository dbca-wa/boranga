# syntax = docker/dockerfile:1.2

# Prepare the base environment.
FROM ghcr.io/dbca-wa/docker-apps-dev:ubuntu2510_base_latest  AS builder_base_boranga

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
    NODE_MAJOR=24 \
    NODE_OPTIONS=--max_old_space_size=4096

FROM builder_base_boranga AS apt_packages_boranga

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install --no-install-recommends -y \
    bzip2 \
    ca-certificates \
    curl \
    g++ \
    graphviz \
    ipython3 \
    libgraphviz-dev \
    python3-venv \
    software-properties-common \
    ssh \
    git \
    sudo
    
RUN update-ca-certificates


FROM apt_packages_boranga AS gdal_boranga

# Install newer gdal version that is secure 
# Doesn't work with ubuntu 25.10 yet
# RUN add-apt-repository ppa:ubuntugis/ubuntugis-unstable && \
#     apt-get update && \
#     apt-get install --no-install-recommends -y \
#     gdal-bin \
#     python3-gdal

FROM gdal_boranga AS node_boranga

# install node
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" \
    | tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && \
    apt-get install -y nodejs
RUN apt clean
FROM node_boranga AS configure_boranga

COPY startup.sh /

RUN chmod 755 /startup.sh && \
    chmod +s /startup.sh && \
    groupadd -g 5000 oim && \
    useradd -g 5000 -u 5000 oim -s /bin/bash -d /app && \
    usermod -a -G sudo oim && \
    echo "oim  ALL=(ALL)  NOPASSWD: /startup.sh" > /etc/sudoers.d/oim && \
    mkdir /app && \
    chown -R oim.oim /app && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    rm -rf /tmp/*

FROM configure_boranga AS python_dependencies_boranga

WORKDIR /app
USER oim
ENV VIRTUAL_ENV_PATH=/app/venv
ENV PATH=$VIRTUAL_ENV_PATH/bin:$PATH

COPY --chown=oim:oim requirements.txt gunicorn.ini.py manage.py python-cron ./
COPY --chown=oim:oim boranga ./boranga

RUN python3 -m venv $VIRTUAL_ENV_PATH
RUN $VIRTUAL_ENV_PATH/bin/pip3 install --upgrade pip && \
    $VIRTUAL_ENV_PATH/bin/pip3 install --no-cache-dir -r requirements.txt && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ /tmp/* /var/tmp/*

FROM python_dependencies_boranga AS build_vue_boranga

RUN cd /app/boranga/frontend/boranga; npm ci --omit=dev && \
    cd /app/boranga/frontend/boranga; npm run build

FROM build_vue_boranga AS collectstatic_boranga

RUN touch /app/.env && \
    $VIRTUAL_ENV_PATH/bin/python manage.py collectstatic --noinput

FROM collectstatic_boranga AS launch_boranga

EXPOSE 8080
HEALTHCHECK --interval=1m --timeout=5s --start-period=10s --retries=3 CMD ["wget", "-q", "-O", "-", "http://localhost:8080/"]
CMD ["/startup.sh"]
