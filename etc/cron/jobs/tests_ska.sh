#!/bin/bash

/etc/profile.d/rucio_init.sh

voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape
wget https://raw.githubusercontent.com/ESCAPE-WP2/rucio-analysis/master/etc/tests_ska.yml -O /tmp/tests_ska.yml
python3 /opt/rucio-analysis/src/run-analysis.py -v -t /tmp/tests_ska.yml
