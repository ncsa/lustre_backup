#!/bin/bash

#  Install: locate this source package somewhere out of the way 
#           like /tmp/lb and run install
#
#  CONFIG:  PREFIX is 
#           BUILDTMP is 
#           INIT
export PREFIX=/usr/local  # where this tool gets installed, dar  too.
export BUILDTMP=/tmp      # writable scratch used for building (dar)
export INITD=/etc/init.d  # for the system init scripts

####################################################################
#
#  1. Setup the runtime parts
#
####################################################################

#
# check installing as root
#
[[ $(id -u) == '0' ]] || (echo "Please run as privileged (su)"; exit -1)

#
# scl for centos has python27 proper dev tools
# If it's not here install this is a first-run,
# install scl and some other things, then use it.
# 2nd time through it will skipt this stanza
#
if [[ -f /etc/centos-release && ! -x /usr/bin/scl  ]]; then
  yum -y update
  yum -y install centos-release-scl
  yum -y update
  yum -y install python27 swig m2crypto devtoolset-6-gcc-c++ \
    bzip2-devel zlib-devel lzo-devel xz-devel libgcrypt-devel \
    autoconf automake glibc-static  libattr-dev sudo
  exec scl enable $(scl -l) $0
fi

#
# Doublecheck the python version
#
PYVERS=$(echo "python --version" | scl enable python27 bash 2>&1 | cut -d' ' -f2)
[[ $PYVERS > '2.7.0' ]] || (echo "Wrong python version: $PYVERS" && exit -1)

#
# python packages 
#
easy_install globus-sdk globusonline-transfer-api-client rpyc ptvsd

#
# Build and install DAR
#
  pushd ${BUILDTMP}
  curl https://newcontinuum.dl.sourceforge.net/project/dar/dar/2.5.11/dar-2.5.11.tar.gz | tar xpBzf -
  cd dar-2.5.11
  ./configure --enable-mode=64 --prefix=${PREFIX} && make -j && make install && make clean
  popd

#
# Install this package
#
for f in `find bin lib -type f`; do
  install -D $f ${PREFIX}/$f
  done
install bin/lustre_backup.rc ${INITD}
ln -s ${INITD}/lustre_backup.rc ${INITD}/lustre_backup_manager 
ln -s ${INITD}/lustre_backup.rc ${INITD}/lustre_backup_service
service lustre_backup_manager start
service lustre_backup_service start
chckonfig lustre_backup_service on
chckonfig lustre_backup_manager on

/sbin/sysctl -w kernel.shmmax=68719476736 || true #docker can't do

rm -rf /tmp/lb
echo "INSTALLATION COMPLETED"


