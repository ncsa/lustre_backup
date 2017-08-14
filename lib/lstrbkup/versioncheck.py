
#
# use at startup to check, re-exec under scl python if needed
#
import os
import sys

VERSION_WANTED = 27
vers = int(sys.version_info[0])*10 + int(sys.version_info[1])
if vers < VERSION_WANTED:
  print '*********** WRONG PYTHON VERSION: {}'.format(vers)
  os.exit(-27)