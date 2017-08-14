
#
# use at startup to check, re-exec under scl python if needed
#
import sys
import logging

VERSION_WANTED = 27
vers = int(sys.version_info[0])*10 + int(sys.version_info[1])
if vers < VERSION_WANTED:
  logging.critical('*********** WRONG PYTHON VERSION: {} for {}'.format(vers, sys.argv))
  print '*********** WRONG PYTHON VERSION: {}'.format(vers)
  sys.exit(-27)