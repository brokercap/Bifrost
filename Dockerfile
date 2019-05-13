FROM centos:centos6.8
MAINTAINER jc3wish "jc3wish@126.com"
RUN yum install -y wget unzip
RUN wget https://github.com/brokercap/Bifrost/releases/download/v1.0.4-release/linux.zip
RUN unzip linux && cd linux && chmod a+x ./Bifrost-server
ENTRYPOINT ["linux//Bifrost-server","start"]
EXPOSE 21036