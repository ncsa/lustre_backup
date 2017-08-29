#!/bin/bash

BASE=/mnt/a/settools/lustrebackup

host=$( hostname )
matches=$( echo $host | grep '^ie' | wc -l )
if [ $matches -ne 1 ]; then
  echo "This host, '$host', does not appear to be an ie node.  Exiting."
  exit 1
fi

#pbin=$(which python)
#echo "Using python binary: $pbin"
#echo -n "  which is version:"
#$pbin --version

#src_fn="$BASE/lustre_backup.py"
src_fn="$BASE/bin/lustre_backup.rc"
for fn in lustre_backup_manager lustre_backup_service; do
  tgt_fn="/etc/init.d/$fn"
  set -x
  rm -f "$tgt_fn"
  ln -s "$src_fn" "$tgt_fn"
  set +x
done

#/usr/bin/easy_install "globusonline-transfer-api-client==0.10.16"

/sbin/sysctl -w kernel.shmmax=68719476736

echo "DONE"
