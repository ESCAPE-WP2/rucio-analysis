#!/bin/bash

echo -n "checking \$RUCIO_ANALYSIS_ROOT is set... "
if [ -z "${RUCIO_ANALYSIS_ROOT}" ]; then
    echo "FAILED"
    exit 1
else
    echo "OK"
fi

while true; do
    read -p "overwrite user crontab? (y/n) " yn
    case $yn in
        [Yy]* ) cat ${RUCIO_ANALYSIS_ROOT}/etc/cron/crontab | envsubst > /tmp/crontab; crontab /tmp/crontab; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer y or n. ";;
    esac
done

