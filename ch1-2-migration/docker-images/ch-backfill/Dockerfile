FROM centos:centos7.4.1708

RUN yum -y install openssh openssh-server openssh-clients vim bc && \
    yum -y clean all

RUN mkdir -p /scripts
RUN mkdir -p /scripts/sql
COPY scripts/*.sh /scripts/
COPY scripts/sql/*.sql /scripts/sql/
RUN chmod a+x /scripts/*.sh
WORKDIR /scripts