import ConfigParser
import datetime
import fcntl
import os
import os.path
import pickle
import random
import re
import time

#local imports
import dar
import eventmanager
from lustrebackupexceptions import *

class ServiceProvider( object ):
	""" Provides global services such as access to config variables,
		global state, directory lock controls, etc.
		Implements the Singleton design pattern.
	"""
	def __new__( cls, *a, **k ):
		if not hasattr( cls, '_inst' ):
			cls._inst = super( ServiceProvider, cls ).__new__( cls, *a, **k )
		return cls._inst

	def __init__( self, *a, **k ):
		if not hasattr( self, "_firsttime" ):
			super( ServiceProvider, self ).__init__( *a, **k )
			self._firsttime_init( *a, **k )
			self._firsttime = False

	def _firsttime_init( self, *a, **k ):
		self.evMgr = eventmanager.EventManager()
		self.cfg = None
		self.allowedBackupTypes = [ dar.FULL ]

	def loadConfig( self, filename ):
		""" Read a config file and save the data internally in the Borg.
			INPUT: fn = string, filename to load from
			OUTPUT: n/a
		"""
		self.cfg = self.readConfig( filename )

	def readConfig( self, filename ):
		""" Read a config file and return the ConfigParser object.
			INPUT: fn = string, filename to load from
			OUTPUT: instance of ConfigParser
		"""
		cfg = ConfigParser.SafeConfigParser( )
		cfg.optionxform = str
		fh = open( filename )
		cfg.readfp( fh )
		fh.close()
		return cfg

	def enableIncrementalBackups( self ):
		self.allowedBackupTypes.append( dar.INCREMENTAL )

	def disableIncrementalBackups( self ):
		self.allowedBackupTypes = [ dar.FULL ]

	def isBackupTypeAllowed( self, buType ):
		rv = False
		if buType in self.allowedBackupTypes:
			rv = True
		return rv

	def mk_absolute_path( self, cfg_varname, basepath, fn=None ):
		rv = os.path.join( 
			getattr( self, cfg_varname ), 
			basepath.lstrip( os.sep ) )
		if fn:
			rv = os.path.join( rv, fn )
		return rv

	def mk_absolute_path_backup_info( self, basepath ):
		return self.mk_absolute_path( "archive_dir", basepath, "backup.info" )

	def mk_datetime_str( self ):
		return datetime.datetime.now().strftime( "%Y%m%d_%H%M%S" )

	def acquireDirLock( self, dn, timeout=5 ):
		# Check for lock file
		if self.isDirLocked( dn ):
			raise LockFileExists( self._dirLockFilename( dn ) )
		# Try to get fcntl lock & write ownership data
		self._writeDirLock( dn, timeout )
		# Paranoid - pause, read lockfile to verify we own it
		time.sleep( random.randint( 1, 5 ) )
		if not self.isDirLockedByThisPid( dn ):
			raise LockFileIntegrityError( self._dirLockFilename( dn ) )

	def releaseDirLock( self, dn ):
		lockfile = self._dirLockFilename( dn )
		if not self.isDirLocked( dn ):
			raise LockFileMissing( lockfile )
		# Verify that we hold the lock
		if not self.isDirLockedByThisPid( dn ):
			raise LockFileHostPidMismatch( lockfile )
		os.remove( lockfile )

	def releaseDirLockByForce( self, dn, reason="none" ):
		lockfile = self._dirLockFilename( dn )
		if not self.isDirLocked( dn ):
			raise LockFileMissing( lockfile )
		savetime = time.strftime( "%Y%m%d.%H%M%S" )
		lockfilesave = "{0}_{1}.forcedrm".format( lockfile, savetime )
		os.rename( lockfile, lockfilesave )
		with open( lockfilesave, "a" ) as f:
			f.write( "\nForced RM by {0} on {1} reason='{2}'\n".format( os.uname()[1], savetime, reason ) )

	def isDirLocked( self, dn ):
		return os.path.isfile( self._dirLockFilename( dn ) )

	def isDirLockedByThisPid( self, dn ):
		fh = open( self._dirLockFilename( dn ), "r" )
		line = fh.readline()
		fh.close()
		data = self._dirLockData()
		return line == data

	def isDirLockedByThisHost( self, dn ):
		fh = open( self._dirLockFilename( dn ), "r" )
		line = fh.readline().split()[0]
		fh.close()
		data = self._dirLockData().split()[0]
		return line == data

	def _writeDirLock( self, dn, timeout=5 ):
		lockfile = self._dirLockFilename( dn )
		fh = open( lockfile, "w" )
		starttime = datetime.datetime.now()
		while True:
			if ( datetime.datetime.now() - starttime ).seconds > timeout:
				raise LockFileBusy( lockfile )
			try:
				fcntl.flock( fh, fcntl.LOCK_EX | fcntl.LOCK_NB )
			except IOError:
				time.sleep( random.random() )
#				raise
			else:
				# Got lock ok
				break
		fh.write( self._dirLockData() )
		fh.flush()
		os.fsync( fh.fileno() )
		fh.close()

	def _dirLockData( self ):
		return "hostname={0} , pid={1}".format( self.get_hostname(), os.getpid() )

	def _dirLockFilename( self, dn ):
		return os.path.join( dn, ".lustrebackup.lck" )

	def get_hostname( self ):
		return os.uname()[1]

	def get_pid( self ):
		return os.getpid()
		
	def getHostnames( self ):
		return self.cfg.items( "HOSTNAMES" )

	def get_topdirs( self ):
		return self.cfg.items( "TOPDIRS" )
	
	def find_matching_hostname( self, hn_str ):
		rv = None
		for (h, val) in self.getHostnames():
			if h == hn_str:
				rv = val
				break
		return rv

	def getBasepathID( self, dn ):
		id = None
		self._loadBasepathIdMap()
		if dn in self.basepath2idMap:
			id = self.basepath2idMap[ dn ]
		return id

	def setBasepathID( self, dn ):
		newid = None
		self._loadBasepathIdMap()
		if dn in self.basepath2idMap:
			newid = self.basepath2idMap[ dn ]
		else:
			newid = self.basepath2idMap[ "LASTID" ] + 1
			self.basepath2idMap[ "LASTID" ] = newid
			self.basepath2idMap[ dn ] = newid
			self._saveBasepathIdMap()
		return newid

	def _loadBasepathIdMap( self ):
		if not hasattr( self, "basepath2idMap" ):
			fn = os.path.join( self.archive_dir, "basepath2id.map" )
			if not os.path.isfile( fn ):
				self._saveBasepathIdMap()
			try:
				self.basepath2idMap = self._readDataFile( fn )
			except EOFError:
				raise ReadFileError( fn )

	def _saveBasepathIdMap( self ):
		if not hasattr( self, "basepath2idMap" ):
			self.basepath2idMap = { "LASTID": 0 }
		fn = os.path.join( self.archive_dir, "basepath2id.map" )
		self._writeDataFile( fn, self.basepath2idMap )

	def _readDataFile( self, fn ):
		rv = None
		with open( fn ) as infile:
			rv = pickle.load( infile )
		return rv

	def _writeDataFile( self, fn, obj ):
		with open( fn, "w" ) as outfile:
			pickle.dump( obj, outfile )

#TODO: Move dar.is_filesize_changing to service_provider?
#	def is_filesize_changing( self, fn, max_wait=2 ):
#		rv = False
#		total_sleep = 0
#		increment = 2
#		while total_sleep <= max_wait:
#			s1 = os.path.getsize( fn )
#			time.sleep( increment )
#			total_sleep += increment
#			s2 = os.path.getsize( fn )
#			if s1 != s2:
#				rv = True
#				break
#		return rv

#	def getLatestFile( self, dirname, re_pattern ):
#		latestdate = datetime.datetime( 1970, 1, 1 )
#		latestfn = None
#		regexp = re.compile( re_pattern )
#		for fn in os.listdir( dirname ):
#			if regexp.search( fn ):
#				abs_fn = os.path.join( dirname, fn )
#				mtime = os.stat( abs_fn ).st_mtime
#				mdate = datetime.datetime.fromtimestamp( mtime )
#				if mdate > latestdate:
#					latestdate = mdate
#					latestfn = fn
#		if not latestfn:
#			latestdate = None
#		return( latestfn, latestdate )


	def setup_logging( self, level ):
		logger = logging.getLogger( "lustrebackup.serviceprovider" )
		logger.setLevel( getattr( logging, level ) )
		syslog_handler = logging.handlers.SysLogHandler( address='/dev/log' )
		logger.addHandler( syslog_handler )
		fmt_str = "%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)s - %(message)s"
		syslog_formatter = logging.Formatter( fmt_str )
		syslog_handler.setFormatter( syslog_formatter )
		logger.addHandler( syslog_handler )
		logger.debug( "Logger setup" )
		logger.info( "Logger setup" )
		logger.warning( "Logger setup" )



	# Provide transparent access to settings in the GENERAL section
	def __getattr__( self, name ):
		if name.startswith( "_" ):
			return super( ServiceProvider, self ).__getattr__( name )
		try:
			rv = self.cfg.get( "GENERAL", name )
		except ConfigParser.NoOptionError:
			raise AttributeError( name )
		return rv

if __name__ == "__main__":
	raise UserWarning( "EventManager cannot be invoked directly" )
