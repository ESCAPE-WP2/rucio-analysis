FROM projectescape/rucio-client:latest

USER root

RUN yum -y install wget vim 
RUN yum -y install git

COPY . /opt/rucio-analysis
RUN python3 -m pip install -r /opt/rucio-analysis/requirements.txt

ENTRYPOINT ["/bin/bash"]
