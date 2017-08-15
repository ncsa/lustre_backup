#
# Docker image builder, primarily for development
#
# Build with docker build . -t lbackup

FROM centos:6.9
COPY . /mnt/a/settools/lustrebackup/
RUN cd /mnt/a/settools/lustrebackup && ./INSTALL_docker.sh
CMD bash
