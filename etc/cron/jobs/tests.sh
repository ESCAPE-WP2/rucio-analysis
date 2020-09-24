#!/bin/bash

/etc/profile.d/rucio_init.sh

# This test script pulls the most recent version of tests.yml from the github.

voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape
wget https://raw.githubusercontent.com/ESCAPE-WP2/rucio-analysis/master/etc/tests.yml -O /tmp/tests.yml
python3 /opt/rucio-analysis/src/run-analysis.py -v -t /tmp/tests.yml
