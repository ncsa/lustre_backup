#!python
import versioncheck
# Standard imports
import rpyc
import os
import os.path
#import datetime
import logging
import logging.config
import sys
import rpyc.utils.server
import subprocess

# Local imports
import serviceprovider
import dar
import events
import filesystemscanner
from lustrebackupexceptions import *

#logging.config.fileConfig( os.path.join( os.path.dirname( __file__), 'logging.service.cfg' ) )
logging.config.fileConfig( os.path.join( 
	os.getenv( "LSTRBKUPCONFDIR" ),
	"logging.service.cfg" ) )

class LustreBackupService( rpyc.Service ):

	#---------------------------------------------------------------#

	class exposed_RemoteLustreBackup( object ):
		def __init__( self, basepath, type, callback, remoteHostID ):
			self.basepath = basepath
			self.type = type
			self.remoteHostID = remoteHostID
			self.callback = callback
			self.darBU = dar.DarBackup( self.basepath, self.type )

		def exposed_isBackupError( self ):
			rv = False
			if "FAIL" in self.darBU.status:
				rv = True
			return rv

		def exposed_isBackupDone( self ):
			rv = False
			if "SUCCESS" in self.darBU.status:
				rv = True
			return rv

		def exposed_startBackup( self ):
			try:
				self.darBU.startBackup()
			except ( BackupInProgress ) as e:
				logging.debug( "RemoteLustreBackup caught exception:{0} ({1})".format( e, self.basepath ) )
			except ( BackupAlreadyCompleted ) as e:
				logging.debug( "RemoteLustreBackup caught exception:{0} ({1})".format( e, self.basepath ) )
				self.callback( "BackupCreateArchiveCompletedEvent", self.basepath )
			except ( BackupBasepathDoesNotExist ) as e:
				# nothing to do here, let ScanForNewAndDeletedPaths
				# handle the missing basepath
				logging.debug( "RemoteLustreBackup caught exception:{0} ({1})".format( e, self.basepath ) )
			else:
				self.callback( "BackupCreateArchiveCompletedEvent", self.basepath )
			self.callback( "RemoteHostCompletedEvent", self.remoteHostID )

		def exposed_startBackupTest( self ):
			logging.debug( ">>>Enter" )
			self.darBU.debug = True
			self.darBU.startBackup()
			self.darBU.debug = False
			logging.debug( "<<<Exit" )
			self.callback( "RemoteHostCompletedEvent", self.remoteHostID )

		def exposed_startCleanup( self ):
			logging.debug( ">>>Enter ({0})".format( self.darBU ) )
			try:
				self.darBU.startCleanup()
			except ( LockFileExists, LockFileBusy ) as e:
				logging.debug( "startCleanup caught exception:{0} ({1})".format( e, self.basepath ) )
				self.callback( "StartCleanupSuccessfulBackupEvent", self.basepath )
			else:
				self.callback( "CleanupSuccessfulBackupCompletedEvent", self.basepath )
			self.callback( "RemoteHostCompletedEvent", self.remoteHostID )
			logging.debug( "<<<Exit" )

	#---------------------------------------------------------------#

	class exposed_RemoteFilesystemScanner( object ):
		def __init__( self, topdir, callback, remoteHostID ):
			self.remoteHostID = remoteHostID
			self.callback = callback
			self.scanner = filesystemscanner.FilesystemScanner( topdir )

		def exposed_startScanForNewAndDeletedDirs( self ):
			logging.debug( ">>>Enter" )
			( new_dirs, deleted_dirs ) = \
				self.scanner.scanForNewAndDeletedDirs()
			for new_dn in new_dirs:
				self.callback( "NewBasepathEvent", new_dn )
			for del_dn in deleted_dirs:
				self.callback( "DeletedBasepathEvent", del_dn )
			self.callback( "RemoteHostCompletedEvent", self.remoteHostID )
			logging.debug( "<<<Exit" )

		def exposed_startScanForFullBackupsNeeded( self ):
			logging.debug( ">>>Enter" )
			try:
				dirlist = self.scanner.scanForFullBackupsNeeded()
			except ( ReadFileError ) as e:
				logging.debug( "caught ReadFileError: '{0}', now returning".format( e ) )
				self.callback( "RemoteHostCompletedEvent", self.remoteHostID )
				return
			if len( dirlist ) > 0:
				for dn in dirlist:
					logging.debug( "  Callback StartFullBackupEvent for {0}".format( dn ) )
					self.callback( "StartFullBackupEvent", dn )
			else:
				logging.debug( "  Callback NoMoreFullBackupsEvent for {0}".format( self.scanner.topdir ) )
				self.callback( "NoMoreFullBackupsEvent", self.scanner.topdir )
			self.callback( "RemoteHostCompletedEvent", self.remoteHostID )
			logging.debug( "<<<Exit" )

		def exposed_startScanForIncrementalBackupsNeeded( self ):
			logging.debug( ">>>Enter" )
			dirlist = self.scanner.scanForIncrementalBackupsNeeded()
			if len( dirlist ) > 0:
				for dn in dirlist:
					self.callback( "StartIncrementalBackupEvent", dn )
			self.callback( "RemoteHostCompletedEvent", self.remoteHostID )
			logging.debug( "<<<Exit" )
			

def run():
	logging.debug( "Starting" )
	cfg_fn = os.path.join( 
		os.getenv( "LSTRBKUPCONFDIR" ), 
		"lustre_backup.cfg" )
	isp = serviceprovider.ServiceProvider()
	isp.loadConfig( cfg_fn )
	this_hn = isp.get_hostname()
	service_ip = isp.find_matching_hostname( this_hn )
	if service_ip is None:
		raise UserWarning( "Cannot determine service_ip for hostname '{0}' using config file '{1}'".format( this_hn, cfg_fn ) )
	logger = logging.getLogger()
	t = rpyc.utils.server.ThreadedServer( 
		LustreBackupService, 
		hostname=service_ip,
		port=int( isp.remote_port), 
		auto_register=False,
		logger = logger )
	t.start()

if __name__ == "__main__":
	#raise SystemExit( "Cannot invoke lustre_backup_service directly." )
	run()
