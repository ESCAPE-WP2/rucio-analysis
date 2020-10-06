FROM projectescape/rucio-client:latest

USER root

RUN yum -y install wget vim 
RUN yum -y install python3, python3-devel, python-devel
RUN yum -y install cmake git
RUN yum -y install boost-python36 boost-python36-devel
RUN yum -y install gfal2 gfal2-devel
RUN yum -y install gcc gcc-c++

COPY . /opt/rucio-analysis
RUN python3 -m pip install -r /opt/rucio-analysis/requirements.txt

ENTRYPOINT ["/bin/bash"]