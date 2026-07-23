# Prepare the base environment.
FROM ubuntu:26.04 as builder_base_container
MAINTAINER asi@dbca.wa.gov.au
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Australia/Perth
ENV PRODUCTION_EMAIL=True
ENV SECRET_KEY="ThisisNotRealKey"
RUN apt-get clean
RUN apt-get update
RUN apt-get upgrade -y
# RUN apt-get install --no-install-recommends -y wget git libmagic-dev gcc binutils libproj-dev gdal-bin python3 python3-setuptools python3-dev python3-pip tzdata cron nginx sudo
RUN apt-get install --no-install-recommends -y wget git curl gnupg2 ca-certificates lsb-release ubuntu-keyring libmagic-dev gcc binutils libproj-dev gdal-bin python3 python3-setuptools python3-dev python3-pip tzdata cron sudo
RUN apt-get install --no-install-recommends -y libpq-dev patch
RUN apt-get install --no-install-recommends -y postgresql-client mtr htop vim 
RUN curl https://nginx.org/keys/nginx_signing.key | gpg --dearmor > /usr/share/keyrings/nginx-archive-keyring.gpg
RUN gpg --dry-run --quiet --no-keyring --import --import-options import-show /usr/share/keyrings/nginx-archive-keyring.gpg | grep -q "573BFD6B3D8FBC641079A6ABABF5BD827BD9BF62"
RUN echo "deb [signed-by=/usr/share/keyrings/nginx-archive-keyring.gpg] https://nginx.org/packages/ubuntu $(lsb_release -cs) nginx" > /etc/apt/sources.list.d/nginx.list
RUN printf "Package: *\nPin: origin nginx.org\nPin: release o=nginx\nPin-Priority: 900\n" > /etc/apt/preferences.d/99nginx
RUN apt-get update && apt-get install --no-install-recommends -y nginx
# Install Python libs from requirements.txt.

# Default Scripts
RUN wget https://raw.githubusercontent.com/dbca-wa/wagov_utils/main/wagov_utils/bin/default_script_installer.sh -O /tmp/default_script_installer.sh
RUN chmod 755 /tmp/default_script_installer.sh
RUN /tmp/default_script_installer.sh

COPY timezone /etc/timezone
ENV TZ=Australia/Perth
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone



# Create local user
RUN groupadd -g 5000 oim
RUN useradd -g 5000 -u 5000 oim -s /bin/bash -d /app
RUN mkdir /app
RUN chown -R oim.oim /app

COPY nginx-default.conf /etc/nginx/sites-enabled/default
# RUN service rsyslog start
COPY cron /etc/cron.d/container
RUN chmod 0644 /etc/cron.d/container
RUN crontab /etc/cron.d/container
RUN service cron start
RUN touch /var/log/cron.log
RUN service cron start
COPY pre_startup.sh startup.sh /
RUN chmod 755 /startup.sh
RUN chmod 755 /pre_startup.sh
RUN mkdir /container-config/
RUN chown -R oim.oim /container-config/

WORKDIR /app
USER oim
# Install the project (ensure that frontend projects have been built prior to this step).
COPY python-cron ./
RUN touch /app/.env

COPY --chown=oim:oim reporting_database_rebuild.sh /
COPY --chown=oim:oim open_reporting_db /

RUN chmod 755 /open_reporting_db
RUN chmod 755 /reporting_database_rebuild.sh

EXPOSE 80
HEALTHCHECK CMD service cron status | grep "cron is running" || exit 1
CMD ["/startup.sh"]
