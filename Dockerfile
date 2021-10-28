ARG BASEIMAGE
ARG BASETAG

FROM $BASEIMAGE:$BASETAG

USER root

RUN yum -y install wget vim python3 python3-devel openssl-devel swig gcc-c++

RUN python3 -m pip install --upgrade pip

COPY requirements.txt /tmp/requirements.txt

RUN python3 -m pip install -r /tmp/requirements.txt

COPY . /opt/rucio-analysis

WORKDIR /opt/rucio-analysis

ENV TASK_FILE_PATH ./opt/rucio-analysis/etc/tasks/test.stubs.yml

#modified requestclient.py for request_history
RUN mv /opt/rucio-analysis/src/dev/requestclient.py /usr/local/lib/python3.6/site-packages/rucio/client/requestclient.py

ENTRYPOINT ["bash", "./etc/init/docker-init.sh"]
