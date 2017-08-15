#!/bin/bash

BASE=/mnt/a/settools/lustrebackup

host=$( hostname )
matches=$( echo $host | grep '^ie' | wc -l )
if [ $matches -ne 1 ]; then
  echo "This host, '$host', does not appear to be an ie node.  Exiting."
  #exit 1
fi

#
# Doublecheck the python version, re-exec under scl if necessary
#
PYVERS=$(python --version 2>&1 | cut -d' ' -f2)
[[ $PYVERS > '2.7.0' ]] || exec scl enable python27 "$0 $@"

easy_install globus-sdk rpyc ptvsd

service lustre_backup_manager stop || true
service lustre_backup_service stop || true


mkdir -p $BASE || true
src_fn="$BASE/bin/lustre_backup.rc"
for fn in lustre_backup_manager lustre_backup_service; do
  tgt_fn="/etc/init.d/$fn"
  set -x
  rm -f "$tgt_fn"
  ln -s "$src_fn" "$tgt_fn"
  set +x
done

chkconfig lustre_backup_service on
chkconfig lustre_backup_manager on
service lustre_backup_manager start
service lustre_backup_service start

echo "DONE"





