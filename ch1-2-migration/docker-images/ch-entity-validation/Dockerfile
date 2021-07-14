FROM centos:centos7.4.1708

RUN yum -y install openssh openssh-server openssh-clients vim bc && \
    yum -y clean all

RUN mkdir -p /scripts
COPY scripts/*.sh /scripts/
RUN chmod a+x /scripts/*.sh
WORKDIR /scripts