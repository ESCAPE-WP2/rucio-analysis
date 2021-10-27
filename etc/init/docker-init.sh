#!/bin/bash

/etc/profile.d/rucio_init.sh

# these are fts client attributes
export X509_USER_KEY=$RUCIO_CFG_CLIENT_KEY 2>/dev/null
export X509_USER_CERT=$RUCIO_CFG_CLIENT_CERT 2>/dev/null

voms-proxy-init --cert $RUCIO_CFG_CLIENT_CERT --key $RUCIO_CFG_CLIENT_KEY --voms $RUCIO_VOMS
python3 /opt/rucio-analysis/src/run.py -v -t $TASK_FILE_PATH
