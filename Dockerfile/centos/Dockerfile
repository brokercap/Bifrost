FROM centos:centos7
MAINTAINER jc3wish "jc3wish@126.com"
ADD ./linux ./linux
RUN cd linux && chmod a+x ./bin/Bifrost* && mkdir -p ./data && mkdir ./logs
ENTRYPOINT ["linux/bin/Bifrost-server","start"]
EXPOSE 21036