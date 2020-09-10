#!/bin/sh
# Authors:
# - Vincent Garonne, <vgaronne@gmail.com>, 2018
# Adapted by Yan Grange, <grange@astron.nl>, 2020

shopt -s checkwinsize

if [ ! -f /opt/rucio/etc/rucio.cfg ]; then
    echo "File rucio.cfg not found. It will generate one."
    j2 /rucio.cfg.escape.j2 > /opt/rucio/etc/rucio.cfg
fi

echo "Enable shell completion on the rucio commands"
eval "$(register-python-argcomplete rucio)"
eval "$(register-python-argcomplete rucio-admin)"