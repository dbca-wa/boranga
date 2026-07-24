# Prepare the base environment.
FROM ghcr.io/dbca-wa/boranga:latest AS builder_base_borangadev

ENV NODE_MAJOR=24

USER root

RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential && \
    apt-get install --no-install-recommends -y git && \
    apt-get install --no-install-recommends -y gdal-bin && \
    apt-get install --no-install-recommends -y graphviz && \
    apt-get install --no-install-recommends -y libffi-dev && \
    apt-get install --no-install-recommends -y libpq-dev && \
    apt-get install --no-install-recommends -y libssl-dev && \
    apt-get install --no-install-recommends -y patch && \
    apt-get install --no-install-recommends -y pkg-config && \
    apt-get install --no-install-recommends -y postgresql-client && \
    apt-get install --no-install-recommends -y python3-dev && \
    apt-get install --no-install-recommends -y python3-pip && \
    apt-get install --no-install-recommends -y python3-pydot && \
    apt-get install --no-install-recommends -y screen && \
    apt-get install --no-install-recommends -y ssh && \
    apt-get install --no-install-recommends -y sudo && \
    apt-get install --no-install-recommends -y virtualenv

# Install Node.js and clean up in the same layer.
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" \
    | tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && \
    apt-get install --no-install-recommends -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN openssl req -new -newkey rsa:4096 -days 3650 -nodes -x509 -subj  "/C=AU/ST=Western Australia/L=Perth/O=Development Environment/OU=IT Department/CN=example.com"  -keyout /etc/ssl/private/selfsignedssl.key -out /etc/ssl/private/selfsignedssl.crt

RUN echo 'if ! [ -n "${SUDO_USER}" -a -n "${SUDO_PS1}" ]; then' >> /etc/bash.bashrc
RUN echo "export PS1='\[\e[;31m\]dev:\[\e[m\] \[\e[;32m\]\u@\h:\w \$ \[\e[m\]'" >> /etc/bash.bashrc
RUN echo "fi" >> /etc/bash.bashrc

# For Linux
RUN groupadd -g 20000 container
RUN useradd -g 20000 -u 200000 container -s /bin/bash
RUN mkdir /home/container
RUN chown container.container /home/container
RUN cp /root/.bashrc /home/container
RUN cp /root/.profile /home/container

ENV PROJ_NETWORK=ON

EXPOSE 8080
HEALTHCHECK --interval=1m --timeout=5s --start-period=10s --retries=3 CMD ["wget", "-q", "-O", "-", "http://localhost:8080/"]
CMD ["/startup.sh"]
