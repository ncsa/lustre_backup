pypathmunge () {
  case ":${PYTHONPATH}:" in
    *:"$1":*)
      ;;
    *)
      if [ "$2" = "after" ] ; then
        PYTHONPATH=$PYTHONPATH:$1
      else
        PYTHONPATH=$1:$PYTHONPATH
      fi
  esac
}

pypathmunge /mnt/a/settools/lustrebackup/lib after

export PYTHONPATH
