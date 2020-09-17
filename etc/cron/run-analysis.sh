#!/bin/bash

now=`date +%Y-%m-%d.%H:%M:%S`

echo $now > /tmp/log_$now

export RUCIO_ANALYSIS_ROOT=/opt/rucio-analysis

docker exec -e -i rucio-analysis voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape | tee -a /tmp/log_$now
docker exec -e RUCIO_ANALYSIS_ROOT=$RUCIO_ANALYSIS_ROOT -i rucio-analysis python3 $RUCIO_ANALYSIS_ROOT/src/run-analysis.py -v -t $RUCIO_ANALYSIS_ROOT/etc/tests.yml >> /tmp/log_$now
