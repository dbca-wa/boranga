#!/bin/bash

# Start the first process
eval $(grep -v '^#' /container-config/.cronenv | xargs -d "\n" -I {} echo export \"{}\" )
sed -i 's/\"/\\"/g' /container-config/.cronenv

if [ -z "$SUDO_OIM_ENCRYPTED_PASSWORD" ]; then
    echo "SUDO_OIM_ENCRYPTED_PASSWORD Ignored":
else
    # create a new password.
    echo "SUDO_OIM_ENCRYPTED_PASSWORD Preparing";
    SUDO_OIM_ENCRYPTED_PASSWORD_SIZE="$(echo $SUDO_OIM_ENCRYPTED_PASSWORD | wc -m)"
    if [ "$SUDO_OIM_ENCRYPTED_PASSWORD_SIZE" -gt "10" ]; then
        usermod -p "$SUDO_OIM_ENCRYPTED_PASSWORD" oim
        echo "SUDO_OIM_ENCRYPTED_PASSWORD Updated";
    fi
fi


if [ $ENABLE_CRON == "True" ];
then
    echo "Starting Cron"
    service cron start &
    status=$?
    if [ $status -ne 0 ]; then
        echo "Failed to start cron: $status"
        exit $status
    fi
else
   echo "ENABLE_CRON environment variable not set to True, cron is not starting."
fi


echo "Starting Nginx"
service nginx start &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start nginx: $status"
  exit $status
fi


