#!/bin/bash

now=`date +%Y-%m-%d.%H:%M:%S`

echo $now > /tmp/log_$now

docker exec -e -it rucio-analysis-prod /bin/bash \
    -c 'voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape' | tee -a /tmp/log_$now
docker exec -e RUCIO_ANALYSIS_ROOT=/opt/rucio-analysis -it rucio-analysis-prod /bin/bash \
    -c 'python3 $RUCIO_ANALYSIS_ROOT/src/run-analysis.py -v -t $RUCIO_ANALYSIS_ROOT/etc/tests.yml' | tee -a /tmp/log_$now