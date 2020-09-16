#!/bin/bash

now=`date +%Y-%m-%d.%H:%M:%S`

docker exec -e -it rucio-analysis-prod /bin/bash \
    -c 'voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape' > /tmp/log_$now
docker exec -e RUCIO_ANALYSIS_ROOT=/opt/rucio-analysis -it rucio-analysis-prod /bin/bash \
    -c 'python3 $RUCIO_ANALYSIS_ROOT/src/run-analysis.py - -t $RUCIO_ANALYSIS_ROOT/etc/tests.yml' > /tmp/log_$now