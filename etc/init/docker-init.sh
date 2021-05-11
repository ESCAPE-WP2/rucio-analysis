#!/bin/bash

/etc/profile.d/rucio_init.sh 

voms-proxy-init --cert $RUCIO_CFG_CLIENT_CERT --key $RUCIO_CFG_CLIENT_KEY --voms $RUCIO_VOMS
python3 /opt/rucio-analysis/src/run.py -v -t $TASK_FILE_PATH
