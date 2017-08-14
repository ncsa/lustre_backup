#!python
import versioncheck

# DEBUG code from
# http://www.dalkescientific.com/writings/diary/archive/2005/04/20/tracing_python_code.html
import sys
import linecache
def traceit(frame, event, arg):
	if event == "line":
		lineno = frame.f_lineno
		filename = frame.f_globals["__file__"]
		if (filename.endswith(".pyc") or
			filename.endswith(".pyo")):
			filename = filename[:-1]
		name = frame.f_globals["__name__"]
		line = linecache.getline(filename, lineno)
		print "%s:%s: %s" % (name, lineno, line.rstrip())
	return traceit
