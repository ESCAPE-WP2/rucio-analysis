FROM rucio/rucio-clients:release-1.22.6.post1

# yum repositories
RUN curl -s -o /etc/yum.repos.d/EGI-trustanchors.repo http://repository.egi.eu/sw/production/cas/1/current/repo-files/EGI-trustanchors.repo

# install packages
RUN yum -y install ca-certificates ca-policy-egi-core
RUN yum -y install wget vim 
RUN yum -y install python3
RUN yum -y install git

# voms setup
RUN mkdir -p /etc/vomses \
    && wget https://indigo-iam.github.io/escape-docs/voms-config/voms-escape.cloud.cnaf.infn.it.vomses -O /etc/vomses/voms-escape.cloud.cnaf.infn.it.vomses \
    && mkdir -p /etc/grid-security/vomsdir/escape \
    && wget https://indigo-iam.github.io/escape-docs/voms-config/voms-escape.cloud.cnaf.infn.it.lsc -O /etc/grid-security/vomsdir/escape/voms-escape.cloud.cnaf.infn.it.lsc

COPY etc/rucio/rucio.cfg.*.j2 / 
COPY etc/docker/bashrc /root/.bashrc
COPY etc/rucio/rucio_cli.sh /etc/profile.d

# create non-priveleged dummy user
RUN useradd -ms /bin/bash user

COPY . /opt/rucio-analysis
RUN python3 -m pip install -r /opt/rucio-analysis/requirements.txt

ENTRYPOINT ["/bin/bash"]