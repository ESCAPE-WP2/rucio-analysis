FROM projectescape/rucio-client:py3

USER root

RUN yum -y install wget vim 
RUN yum -y install python3
RUN yum -y install git

COPY . /opt/rucio-analysis
RUN python3 -m pip install -r /opt/rucio-analysis/requirements.txt

ENTRYPOINT ["/bin/bash"]
