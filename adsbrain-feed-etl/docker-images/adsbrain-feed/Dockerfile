FROM debian:stable-slim

RUN apt-get update && apt-get install -y openssh-client vim bc lftp xlsx2csv

RUN mkdir -p /scripts
RUN mkdir -p /data
COPY scripts/*.sh /scripts/
RUN chmod a+x /scripts/*.sh
WORKDIR /scripts