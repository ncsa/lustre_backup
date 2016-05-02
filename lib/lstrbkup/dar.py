import ConfigParser
import datetime
import os
import os.path
import subprocess
import time
import re
import mover
import logging

#local imports
import serviceprovider
from lustrebackupexceptions import *
import infofile
import backup_dir


#TODO - Add compression ?
#TODO - Add get_backup_filename() function, returns darfile with .1.dar added
#TODO - Should we test each backup after it's completed? Time is
#       likely the limiting factor. How long will it take?
#TODO - Set Lustre backup options in external config file
#TODO - Should dar specific options go in /etc/darrc or in global config?
#TODO - Define exclusions?


CREATE_STARTED = "create_started"
CREATE_FINISHED = "create_finished"
CREATE_FAILED = "create_failed"
CATALOG_STARTED = "catalog_started"
CATALOG_FINISHED = "catalog_finished"
CATALOG_FAILED = "catalog_failed"
DAR_COMPLETE = "dar_complete"
BACKUP_COMPLETE = "backup_complete"
FULL = "FULL"
INCREMENTAL = "INCR"
INCR = INCREMENTAL


class DarBackup( object ):
	SUBPROCESS_INACTIVITY_TIMEOUT = 14400
	def __init__( self, basepath, typ ):
		self.isp = serviceprovider.ServiceProvider()
		self.basepath = basepath
		self.bkup_type = typ
		self.archdir = self.isp.mk_absolute_path( "archive_dir", 
			self.basepath )
		self.mk_dir( self.archdir )
		self.workdir = self.isp.mk_absolute_path( "work_dir", 
			self.basepath )
		self.mk_dir( self.workdir )
		info_filename = self.isp.mk_absolute_path_backup_info( self.basepath )
		self.info = infofile.InfoFile( info_filename )
		self.debug = False


	@classmethod
	def strip_dar_exts( cls, fn, suf_re="\.[0-9]+\.dar$" ):
		return re.sub( suf_re, "", fn )


	def __repr__( self ):
		return "<DarBackup (self.basepath)>".format()

	def startBackup( self ):
		logging.debug( ">>>Enter" )
		
		caught_err = None
		try:
			self.isp.acquireDirLock( self.archdir )
		except ( LockFileExists, LockFileBusy ) as e:
			logging.debug( "..(basepath={0}) caught exception: {1}".format( self.basepath, e ) )
			raise BackupInProgress()
		# Check for old or current backup
		if os.path.isfile( self.info.filename ):
			try:
				self._recover_old_backup()
			except ( BackupAlreadyCompleted ) as caught_err:
				pass
		else:
			self.info.create_ok = True
			self._init_bkup_info()
			self._mk_archive()
			self._mk_catalog()
			self._set_status( DAR_COMPLETE )
		self.isp.releaseDirLock( self.archdir )
		if caught_err is not None:
			logging.debug( "..about to raise err:{0}".format( caught_err ) )
			raise caught_err
		logging.debug( "<<<Exit" )


	def startCleanup( self ):
		logging.debug( ">>>Enter" )
		err = None
		try:
			self.isp.acquireDirLock( self.archdir )
		except ( LockFileExists, LockFileBusy ) as e:
			logging.debug( "({0}) caught exception: {1}".format( self.basepath, e ) )
			raise

		if os.path.isfile( self.info.filename ):
			# Check status (don't want to accidentally clean up a backup
			# that is not ready, which could happen if multiple "cleanup"
			# events are generated)
			status = self.info.status
			if status in ( mover.TRANSFER_FINISHED, BACKUP_COMPLETE ):
				self._set_status( BACKUP_COMPLETE )
				err = self._do_cleanup()

		self.isp.releaseDirLock( self.archdir )
		if err is not None:
			logging.debug( "..about to raise err:{0}".format( err ) )
			raise err
		logging.debug( "<<<Exit" )


	def _recover_old_backup( self ):
		logging.debug( ">>>Enter ({0})".format( self.basepath ) )
		logging.debug( "..Old darfile={0}".format( self.info.darfile ) )
		if self.info.status in ( CREATE_STARTED ):
			if os.path.isfile( self.info.darfile ):
				if self._is_filesize_changing( 
						self.info.darfile, 
						self.SUBPROCESS_INACTIVITY_TIMEOUT ):
					err = BackupInProgress()
					logging.debug( "<<<Exit (about to raise '{0}')".format( err ) )
					raise err
				os.unlink( self.info.darfile )
			self._init_bkup_info()
			self._mk_archive()
			self._mk_catalog()
			self._set_status( DAR_COMPLETE )
		elif self.info.status in ( CREATE_FINISHED ):
			self._init_bkup_host_info()
			self._mk_catalog()
			self._set_status( DAR_COMPLETE )
		elif self.info.status in ( CATALOG_STARTED ):
			if self._is_filesize_changing( 
					self.info.catfile, 
					self.SUBPROCESS_INACTIVITY_TIMEOUT ):
				err = CatalogInProgress()
				logging.debug( "<<<Exit (about to raise '{0}')".format( err ) )
				raise err
			os.unlink( self.info.catfile )
			self._init_bkup_host_info()
			self._mk_catalog()
			self._set_status( DAR_COMPLETE )
		elif self.info.status in ( CATALOG_FINISHED ):
			self._set_status( DAR_COMPLETE )
		elif self.info.status in ( CREATE_FAILED, CATALOG_FAILED ):
			msg = "<<<Exit ..in dar recovery, previous status was fail, so new work will not be attempted."
			err = BackupFailed( msg )
			logging.warning( "BackupFailed: '{0}'".format( err ) )
			raise err
#		elif self.info.status in ( DAR_COMPLETE ):
#			err = BackupAlreadyCompleted( self.basepath )
#			logging.debug( "<<<Exit ..about to raise '{0}'".format( err ) )
#			raise err
#		else:
#			# Status is unknown, which is effectively the same as
#			# AlreadyCompleted, since there is nothing to do
#			err = BackupAlreadyCompleted( self.basepath )
#			logging.debug( "<<<Exit ..about to raise '{0}'".format( err ) )
#			raise err
		# Status is either DAR_COMPLETE or unknown, which is
		# effectively the same since there is nothing to do
		err = BackupAlreadyCompleted( self.basepath )
		logging.debug( "<<<Exit ..about to raise '{0}'".format( err ) )
		raise err


	def _do_cleanup( self ):
		""" Rename backup.info file to retain statistics.
		"""
		logging.debug( "started ({0}) ({1}_{2})".format( 
			self.basepath,
			self.info.date_time,
			self.info.type ) )
		err = None
#		old_path = self.info.filename
#		( dn, fn ) = os.path.split( old_path )
#		prefix = self.info.date_time
#		new_fn = "{0}.{1}".format( prefix, fn )
#		new_path = os.path.join( dn, new_fn )
#		logging.debug( "..rename old_path='{0}' new_path='{1}'".format( 
#			old_path, new_path ) )
		try:
#			os.rename( old_path, new_path )
			self.info.rename( 
				reason=BACKUP_COMPLETE,
				pfx=self.info.date_time )
		except OSError as e:
			err = BackupCleanupError( self.basepath, "While trying to \
backup_info.rename, OSError='{0}".format( e ) )
		if err:
			logging.warn( err )
		logging.debug( "completed ({0}) ({1}_{2})".format( 
			self.basepath,
			self.info.date_time,
			self.info.type ) )
		return err


	def _init_bkup_info( self ):
		"""Initialize info that is stored in external config file
		"""
		self._init_bkup_host_info()
		dar_ref_prefix = ""
		if self.bkup_type == INCREMENTAL:
			ref = self._find_reference_file()
			dar_ref_prefix = DarBackup.strip_dar_exts( ref )
		dar_fn_prefix = self._mk_filename( self.workdir, "dar")
		cat_fn_prefix = self._mk_filename( self.archdir, "cat")
		self.info.update( { 
			"status": "",
			"type": self.bkup_type,
			"dar_ref_prefix": dar_ref_prefix,
			"dar_fn_prefix": dar_fn_prefix,
			"darfile": dar_fn_prefix + ".1.dar", #TODO: call func for absfilename
			"cat_fn_prefix": cat_fn_prefix,
			"catfile": cat_fn_prefix  + ".1.dar", #TODO: call func for absfilename
			} )


	def _init_bkup_host_info( self ):
		"""Set host-specific info.  During recovery, updates only host
		date and log information while keeping dar specific data
		the same as before.
		"""
		self.info.update( {
			"pid": "",
			"host": self.isp.get_hostname(),
			"date_time": self.isp.mk_datetime_str(),
			} )
		self.info.update( {
			"logfile": self._mk_filename( self.archdir, "log"),
			"errfile": self._mk_filename( self.archdir, "err"),
			} )


	def _mk_archive( self ):
		logging.debug( ">>>Enter ({0})".format( self.basepath ) )
		logging.info( "started ({0}) ({1}_{2})".format( 
			self.basepath,
			self.info.date_time,
			self.info.type ) )
		cmd = [ self.isp.dar_cmd ]
		cmd.extend( [ "-c", self.info.dar_fn_prefix ] )          #create
		cmd.extend( [ "-R", self.basepath ] )         #root
		if len( self.info.dar_ref_prefix ) > 1:
			cmd.extend( [ "-A", self.info.dar_ref_prefix ] ) #reference catalog 
		cmd.append( "-Q" )                    #force non-interactive mode
		cmd.append( "-v" )
		self.info.set( "status", CREATE_STARTED )
		#logging.debug( "..cmd=({0})".format( cmd ) )
		starttime = datetime.datetime.now()
		try:
			subp = self._run_cmd( cmd, monitor_output_file=self.info.darfile )
		except ( subprocess.CalledProcessError ) as e:
			self.info.set( "status", CREATE_FAILED )
			logging.error( e )
			raise e
		except ( HungProcessError ) as e:
			self.info.set( "status", CREATE_FAILED )
			logging.error( e )
			raise e
		rc = subp.returncode
		if rc is None:
			#should never get here
			e = DarBackupError( self.basepath, 
			"FATAL ERROR: subprocess says complete but returncode is None" )
			logging.error( e )
			raise e
		elif rc == 0:
			pass
		elif rc == 11:
			logging.warning( "Dar exited with error code 11; backup completed but some files changed during backup and therefore may not be valid." )
		elif rc == 5:
			# Check that basepath exists 
			if not os.path.exists( self.basepath ):
				# this may be ok, such as if user account was removed,
				# but still need to exit
				e = BackupBasepathDoesNotExist( self.basepath )
				logging.warning( e )
				raise e
			else:
				e = DarBackupError( 
					self.basepath, 
					"FATAL ERROR: Dar exited with error code 5.  Assume filesystem problem." )
				logging.error( e )
				raise e
		else:
			self.info.set( "status", CREATE_FAILED )
			e = DarBackupError( 
				self.basepath, 
				"dar exited with returncode='{0}'".format( rc ) )
			logging.error( e )
			raise e
		self.info.set( "status", CREATE_FINISHED )
		endtime = datetime.datetime.now()
		self._save_stats( starttime, endtime, "mk_archive" )
		logging.info( "completed ({0}) ({1}_{2})".format( 
			self.basepath,
			self.info.date_time,
			self.info.type ) )
		logging.debug( "<<<Exit ({0})".format( self.basepath ) )


	def _mk_catalog( self ):
		logging.debug( ">>>Enter ({0})".format( self.basepath ) )
		logging.info( "started ({0}) ({1}_{2})".format( 
			self.basepath,
			self.info.date_time,
			self.info.type ) )
		cmd = [ self.isp.dar_cmd ]
		cmd.extend( [ "-C", self.info.cat_fn_prefix ] )	#create catalog
		cmd.extend( [ "-A", self.info.dar_fn_prefix ] )	#reference backup
		#cmd.append( "-e" )						#test
		self.info.set( "status", CATALOG_STARTED )
		starttime = datetime.datetime.now()
		try:
			subp = self._run_cmd( cmd, monitor_output_file=self.info.catfile )
		except ( subprocess.CalledProcessError ) as e:
			self.info.set( "status", CATALOG_FAILED )
			logging.error( e )
			raise e
		except ( HungProcessError ) as e:
			self.info.set( "status", CATALOG_FAILED )
			logging.error( e )
			raise e
		rc = subp.returncode
		if rc is None:
			#should never get here
			e = DarBackupError( self.basepath, 
			"FATAL ERROR: subprocess says complete but returncode is None" )
			logging.error( e )
			raise e
		elif rc == 0:
			pass
		elif rc == 11:
			logging.warning( "Dar exited with error code 11; backup completed but some files changed during backup and therefore may not be valid." )
		elif rc == 5:
			# Check that basepath exists 
			if not os.path.exists( self.basepath ):
				# this may be ok, such as if user account was removed
				e = BackupBasepathDoesNotExist( self.basepath )
				logging.warning( e )
				raise e
			else:
				e = DarBackupError( 
					self.basepath, 
					"FATAL ERROR: Dar exited with error code 5.  Assume filesystem problem." )
				logging.error( e )
				raise e
		else:
			self.info.set( "status", CATALOG_FAILED )
			e = DarBackupError( 
				self.basepath, 
				"dar exited with returncode='{0}'".format( rc ) )
			logging.error( e )
			raise e
		endtime = datetime.datetime.now()
		self._save_stats( starttime, endtime, "mk_catalog" )
		logging.info( "completed ({0}) ({1}_{2})".format( 
			self.basepath,
			self.info.date_time,
			self.info.type ) )
		logging.debug( "<<<Exit ({0})".format( self.basepath ) )


	def _run_cmd( self, cmd, monitor_output_file=None ):
		""" Run command, using subprocess module, with stdout
			and stderr redirected to local files.
			Return (completed) subprocess instance.
		"""
		logging.debug( ">>>Enter" )
		logging.debug( "..cmd=({0})".format( cmd ) )
		err = None
		logf = open( self.info.logfile, 'a' )
		errf = open( self.info.errfile, 'a' )
		subp = subprocess.Popen( cmd, stdout=logf, stderr=errf )
		logging.debug( "..pid=({0})".format( subp.pid ) )
		self.info.set( "pid", subp.pid )
		try:
			self._monitor_subprocess( 
				subp, 
				output_fn=monitor_output_file,
				inactivity_timeout_secs=self.SUBPROCESS_INACTIVITY_TIMEOUT
				)
		except ( HungProcessError ) as err:
			self._stop_subprocess( subp )
		logging.debug( "..cmd ended" )
		logf.close()
		errf.close()
		if err is not None:
			raise err
		logging.debug( "<<<Exit" )
		return subp


	def _monitor_subprocess( self, subp, output_fn=None, inactivity_timeout_secs=3600, check_interval_secs=60 ):
		logging.debug( ">>>Enter" )
		filesize_prev = 0
		secs_since_last_change = 0
		elapsed_secs = 0
		# Check for hung/blocked process
		# poll() returns None while process is still running
		rc = subp.poll()
		while rc is None:
			secs_since_last_change += check_interval_secs
			if output_fn is not None:
				if os.path.isfile( output_fn ):
					# Check if file size changed
					filesize_new = os.path.getsize( output_fn )
					if filesize_new > filesize_prev:
						filesize_prev = filesize_new
						secs_since_last_change = 0
				if secs_since_last_change > inactivity_timeout_secs:
					# Assume blocked/hung process
					err = HungProcessError( 
						self.info.host, 
						self.info.pid, 
						"Output file ({0}) hasnt changed in ({1}) secs. Possible hung or blocked process.".format( output_fn, secs_since_last_change ) )
					logging.warning( err )
					raise err
			logging.debug( "filename={0} size={1} lastchange={2} elapsed={3}".format(
				output_fn,
				filesize_prev,
				secs_since_last_change, 
				elapsed_secs ) )
			time.sleep( check_interval_secs )
			elapsed_secs += check_interval_secs
			rc = subp.poll()
#		else:
#			logging.debug( "..output_fn check failed; output_fn={0}".format( output_fn ) )
		logging.debug( "<<<Exit" )


	def _is_filesize_changing( self, fn, max_wait=2 ):
		logging.debug( ">>>Enter (fn={0}, max_wait={1})".format( fn, max_wait ) )
		rv = False
		total_sleep = 0
		logging.debug( "..total_sleep={0}".format( total_sleep ) )
		increment = 2
		while total_sleep <= max_wait:
			s1 = os.path.getsize( fn )
			time.sleep( increment )
			total_sleep += increment
			logging.debug( "..after increment .. total_sleep={0}".format( total_sleep ) )
			s2 = os.path.getsize( fn )
			if s1 != s2:
				rv = True
				break
		logging.debug( "<<<Exit" )
		return rv


	def _stop_subprocess( self, subp ):
		"""attempt to terminate process cleanly
		"""
		logging.debug( ">>>Enter" )
		logging.debug( "..first try subp.terminate()" )
		subp.terminate()
		interval = 10
		max = 900
		counter = 0
		# if dar is listening, a single SIGTERM means stop, cleanup,
		# and exit.  dar will try to create a useful archive file,
		# which will take a little bit of time.
		while counter < max:
			logging.debug( "..counter='{0}'".format( counter ) )
			rc = subp.poll()
			logging.debug( "..result of poll is rc='{0}'".format( rc ) )
			if rc is not None:
				break
			logging.debug( "..about to sleep for '{0}' secs".format( interval ) )
			time.sleep( interval )
			counter += interval
		rc = subp.poll()
		if rc is None:
			#If dar is listening, a second SIGTERM means exit immediately
			logging.debug( "..second try terminate" )
			subp.terminate()
			time.sleep( 10 )
			rc = subp.poll()
			if rc is None:
				logging.debug( "..calling kill" )
				subp.kill()
		logging.debug( "<<<Exit" )


	def _save_stats( self, starttime, endtime, prefix ):
		diff = endtime - starttime
		diff_seconds = diff.days * 86400 + diff.seconds
		if diff_seconds < 1:
			logging.warning( "..elapsed seconds less than 1: value='{0}', starttime='{1}', endtime='{2}'".format( diff_seconds, starttime, endtime ) )
			diff_seconds = 1
		size = os.path.getsize( self.info.darfile )
		rate = size * 1.0 / diff_seconds
		self.info.update( { 
			"{0}_elapsed_seconds".format( prefix ): diff_seconds,
			"{0}_size".format( prefix ): size,
			"{0}_rate_bps".format( prefix ): rate } )


	def _set_status( self, new_status=None ):
		if new_status:
			self.info.set( "status", new_status )


	def mk_dir( self, abspath ):
		if not os.path.isdir( abspath ):
			os.makedirs( abspath, 0770 )


	def _find_reference_file( self ):
#		regexp_pattern = "\d{8}_\d{6}_(" + FULL + "|" + INCREMENTAL + ").cat"
#		( filename, filedate ) = self.isp.getLatestFile( 
#			self.archdir, regexp_pattern )
		bu_dir = backup_dir.Backup_Dir( self.archdir )
		( bkup_date, bkup_info ) = bu_dir.last_successful_backup()
#		if not last_cat_fn:
#			raise DarBackupError( self.basepath, "Unable to find a reference file." )
#		return os.path.join( self.archdir, filename )
		return bkup_info.cat_fn_prefix


	def _mk_filename( self, absdn, suffix=None ):
		dt = self.info.date_time
		fn = "{date}_{typ}{suf}".format( date=dt, typ=self.bkup_type, suf="."+suffix )
		return os.path.join( absdn, fn )


if __name__ == "__main__":
	isp = serviceprovider.ServiceProvider()
	isp.loadConfig( "lustre_backup.cfg" )
	#dar_bu = DarBackup( "/home/logsurfer", FULL )
	dar_bu = DarBackup( "/home/logsurfer", INCREMENTAL )
	dar_bu.startBackup()
