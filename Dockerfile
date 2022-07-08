ARG BASE_RUCIO_CLIENT_IMAGE
ARG BASE_RUCIO_CLIENT_TAG

FROM $BASE_RUCIO_CLIENT_IMAGE:$BASE_RUCIO_CLIENT_TAG

ENV RUCIO_ANALYSIS_ROOT /opt/rucio-analysis

USER root

# repo for oidc-agent
RUN wget https://repo.data.kit.edu/data-kit-edu-centos7.repo -O /etc/yum.repos.d/data-kit-edu-centos7.repo

RUN yum -y install wget vim python3 python3-devel openssl-devel swig gcc-c++ oidc-agent

RUN python3 -m pip install --upgrade pip

COPY requirements.txt /tmp/requirements.txt

RUN python3 -m pip install -r /tmp/requirements.txt

COPY --chown=user . ${RUCIO_ANALYSIS_ROOT}

WORKDIR ${RUCIO_ANALYSIS_ROOT}

ENV TASK_FILE_RELPATH etc/tasks/stubs.yml

USER user

ENTRYPOINT ["bash", "./etc/docker/docker-entrypoint.sh"]
