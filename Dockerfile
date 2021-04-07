FROM projectescape/rucio-client:latest

USER root

RUN yum -y install wget vim python3

COPY . /opt/rucio-analysis

WORKDIR /opt/rucio-analysis

RUN python3 -m pip install -r /opt/rucio-analysis/requirements.txt

ENV TASK_FILE_PATH ./opt/rucio-analysis/etc/tasks/test.stubs.yml

ENTRYPOINT ["bash", "./etc/init/docker-init.sh"]
