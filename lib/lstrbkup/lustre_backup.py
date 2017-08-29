
import versioncheck
import os
import os.path
import subprocess
import sys
import pprint

#local imports
import daemon

cmd = None
allowed_cmds = ( "lustre_backup_manager", "lustre_backup_service" )
allowed_actions = ( 
	"errshow", 
	"errpeek", 
	"foreground", 
	"hist", 
	"logshow", 
	"logpeek", 
	"logrotate", 
	"logrotateforce",
	"restart", 
	"rmall", 
	"rmerrs", 
	"rmlogs", 
	"start", 
	"status", 
	"stop", 
	)
logdir = "/var/log"

def method_builder( func, *a ):
	""" Wrap a function to match the calling signature of a class
		method (ie: accepts self as the first argument).
	"""
	def m( self, *a ):
		func( *a )
	m.__name__ = func.__name__
	return m

def usage( msg=None ):
	if msg:
		print( "Error: {0}".format( msg ) )
	print( "Usage: {0} {1}".format( cmd, "|".join( allowed_actions ) ) )


def get_logfile_list():
	logfiles = {}
	files = os.listdir( logdir )
	for f in files:
		if f.startswith( "{0}.log".format( cmd ) ):
			fname = os.path.join( logdir, f )
			mtime = os.path.getmtime( fname )
			logfiles[ mtime ] = fname
	return [ logfiles[k] for k in sorted( logfiles.keys() ) ]


def get_errfile_list():
	errfiles = {}
	files = os.listdir( logdir )
	for f in files:
		if f.startswith( "{0}.err".format( cmd ) ):
			fname = os.path.join( logdir, f )
			mtime = os.path.getmtime( fname )
			errfiles[ mtime ] = fname
	return [ errfiles[k] for k in sorted( errfiles.keys() ) ]


def logpeek():
	cmdlist = [ "tail", "-n", "2", "/var/log/{0}.log".format( cmd ) ]
	return subprocess.call( cmdlist )


def logshow():
	cmdlist = [ "cat" ]
	cmdlist.extend( get_logfile_list() )
	return subprocess.call( cmdlist )


def errpeek():
	cmdlist = [ "tail", "-n", "2", "/var/log/{0}.err".format( cmd ) ]
	return subprocess.call( cmdlist )
	

def errshow():
	cmdlist = [ "cat" ]
	cmdlist.extend( get_errfile_list() )
	return subprocess.call( cmdlist )


def rmlogs():
	logfiles = get_logfile_list()
	for f in logfiles:
		os.unlink( f )


def rmerrs():
	errfiles = get_errfile_list()
	for f in errfiles:
		os.unlink( f )


def rmall():
	rmlogs()
	rmerrs()


def hist():
	shcmd = "ls -tr /var/log/{0}.log.* | xargs -n 1 grep -v 'DEBUG\|server.accept\|server._serve_client'".format( cmd )
	return subprocess.call( shcmd, shell=True )


def logrotate_cfg_filename():
	return os.path.join( 
		os.getenv( "LSTRBKUPCONFDIR" ),
		"{0}.logrotate.cfg".format( cmd ) 
		)


def logrotate():
	cfg_fn = logrotate_cfg_filename()
	return subprocess.call( [ "logrotate", cfg_fn ] )


def logrotateforce():
	cfg_fn = logrotate_cfg_filename()
	return subprocess.call( [ "logrotate", "-f", cfg_fn ] )


def checkenv( name ):
	if not os.getenv( name ):
		raise SystemExit( "Missing environment variable '{0}'".format( name ) )


if __name__ == "__main__":
	# check for necessary environment settings
	for varname in [ "LSTRBKUPCONFDIR" ]:
		checkenv( varname )

	# check for valid cmd
#	pprint.pprint( sys.argv )
	try:
		cmd = os.path.basename( sys.argv[1] )
	except ( IndexError ) as e:
		raise SystemExit( "Missing command; expected one of ({1})".format( cmd, "|".join( allowed_cmds ) ) )
	if cmd not in allowed_cmds:
		raise SystemExit( "Unknown cmd name '{0}'; expected one of ({1})".format( cmd, "|".join( allowed_cmds ) ) )

	# check for valid action
	try:
		action = sys.argv[2]
	except ( IndexError ) as e:
		usage( "Missing action." )
		sys.exit( 2 )
	if action not in allowed_actions:
		usage( "Unknown action: '{0}'".format( action ) )
		sys.exit( 3 )

	# if action is a local action (ie: something defined above), run it, then exit
	globls = globals()
	if action in globls:
		rv = globls[ action ]()
		sys.exit( rv )

	# the action is not a locally defined function, it must be a
	# method defined in the class who's name matches the program name
	# for this scripts invocation (ie: sys.argv[1])
	# import module
	cmd_parts = cmd.split( "_" )
	module_name = "".join( cmd_parts )
	module = __import__( module_name )

	# define class
	cls_name = "_".join( [ s.capitalize() for s in cmd_parts ] + [ "Daemon" ] )
	cls = type( cls_name, (daemon.Daemon,), {} )
	# override the run function
	new_run = method_builder( module.run )
	setattr( cls, new_run.__name__, new_run )
	# create class instance
	pid_filename = "/var/run/{0}.pid".format( module_name )
	sout = "/var/log/{0}.out".format( cmd )
	serr = "/var/log/{0}.err".format( cmd )
	daemon = cls( pid_filename, stdout=sout, stderr=serr )

	# get function matching action
	try:
		func = getattr( daemon, action )
	except AttributeError:
		usage( "Command '{0}' not found.".format( action ) )
		sys.exit( 4 )
	#print( "found func '{0}'".format( func ) )
	func()
	sys.exit( 0 )
