#!/bin/bash

/etc/profile.d/rucio_init.sh

voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms $RUCIO_VOMS
python3 /opt/rucio-analysis/src/run-analysis.py -v -t $TASK_FILE_PATH
