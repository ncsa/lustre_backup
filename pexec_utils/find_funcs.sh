#!/bin/bash

function mk_pexec_list {
  nodelist=$( echo $* | tr ' ' ',' )
  echo "${nodelist}"
}
