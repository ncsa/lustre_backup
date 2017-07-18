#
# Docker image builder, primarily for development
#
# Build with docker build . -t lbackup

FROM centos:6.9
COPY . /tmp/lb/
RUN cd /tmp/lb && ./INSTALL.sh && rm -rf /tmp/lb
CMD bash
