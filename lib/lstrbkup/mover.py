#native imports
import pickle
import os
import os.path
import logging

#local imports
import dar
import eventmanager
import events
import infofile
import serviceprovider
import transfer
from lustrebackupexceptions import *

TRANSFER_STARTED = "txfr_started"
TRANSFER_FINISHED = "txfr_finished"
TRANSFER_FAILED = "txfr_failed"

class Mover( object ):
	""" Manage the movement of files between disk storage and archive
		(LTS) systems.
		Using globus online: initiate and new transfers (tasks).  Generate
		events for both completed and failed GO tasks.
	"""
	def __init__( self ):
		self.isp = serviceprovider.ServiceProvider()
		self.evMgr = eventmanager.EventManager()
		self.transfer_info_file = infofile.InfoFile( 
			self.isp.transfer_info_file, 
			create_ok=True )
		self.evMgr.registerListener( {
			'events.StartTransferEvent': self.start_new_transfer,
			'events.StartOfSemidayEvent': self.check_open_transfers,
			'events.TransferCompletedEvent': self.cleanup_transfer,
			'events.TransferFailedEvent': self.transfer_failed_handler,
			} )

	def start_new_transfer( self, ev ):
		""" Handler for StartNewTransferEvent.
		"""
		logging.debug( ">>>Enter" )
		# Try to lock dir
		basepath = ev.path
		archdir = self.isp.mk_absolute_path( "archive_dir", basepath )
		try:
			self.isp.acquireDirLock( archdir )
		except ( LockFileExists, LockFileBusy ):
			logging.warning( "..Cannot start transfer, locked dir = '{0}'".format( archdir ) )
			self.evMgr.delayPost( ev )
			return

		# Missing backup.info file is ok, it may have already been
		# cleaned up.  This will occur when backups are restarted when
		# a transfer has already completed; a race condition occurs
		# between check_open_transfers and start_new_backup.
		err = None
		bkup_info_fn = self.isp.mk_absolute_path_backup_info( basepath )
		if os.path.exists( bkup_info_fn ):
			# Check status
			bkup_info = infofile.InfoFile( bkup_info_fn )
			status = bkup_info.status
			logging.debug( "..found status: '{0}'".format( status ) )
			if status in ( dar.DAR_COMPLETE ):
				try:
					self._start_new_txfr( basepath, bkup_info )
				except ( TransferError ) as e:
					err = e
			elif status in ( TRANSFER_STARTED, TRANSFER_FINISHED, TRANSFER_FAILED ):
				try:
					self._recover_txfr( status, basepath, bkup_info )
				except ( TransferError ) as e:
					err = e
			else:
				err = UserWarning( "Unknown backup_status '{0}' in start_new_transfer for basepath '{1}'".format( status, basepath ) )
			# Handle exceptions
			if err is not None:
				try:
					task_id = err.task_id
				except ( KeyError, AttributeError ):
					task_id = None
				self.evMgr.post( events.TransferFailedEvent( task_id, basepath ) )
				bkup_info.update( {
					"status": TRANSFER_FAILED,
					"last_error": str( err ),
				} )
				logging.warning( err )

		# Release dir lock
		self.isp.releaseDirLock( archdir )

		logging.debug( "<<<Exit" )
		

	def check_open_transfers( self, ev ):
		""" Handler for StartOfSemidayEvent
		"""
		logging.debug( ">>>Enter" )
		# Get list of transfers
		pickled_txfrs = self.transfer_info_file.as_dict()
		for t_id, t_ps in pickled_txfrs.iteritems():
			t = pickle.loads( t_ps )
			try:
				self._update_transfer( t )
			except ( TransferLoadError ) as e:
				self.evMgr.post( events.TransferFailedEvent( t.task_id, t.basepath ) )
				continue
			self._initiate_cleanup_if_complete( t )
		logging.debug( "<<<Exit" )


	def _update_transfer( self, t ):
		""" Update transfer info from globusonline and save to local
			file.  Update last_status_check in backup_info file.
		"""
		# Update transfer details
		t.update()
		# Update local transfer_list_file
		self._save_transfer( t )
		# Update last_status_check
		bkup_info_fn = self.isp.mk_absolute_path_backup_info( t.basepath )
		bkup_info = infofile.InfoFile( bkup_info_fn )
		bkup_info.set_datetime( 'last_status_check', t.last_update )



	def _initiate_cleanup_if_complete( self, t ):
		# Send cleanup event if transfer is complete
		if t.is_done():
			self.evMgr.post( events.TransferCompletedEvent( t.task_id, t.basepath ) )
		

	def _start_new_txfr( self, basepath, bkup_info ):
		# Get information about file to be transferred
		fn_src = bkup_info.darfile
		fn_src_bn = os.path.basename( fn_src )
		fn_dst = self.isp.mk_absolute_path( 
			"lts_basedir",
			basepath,
			fn_src_bn
			)

		# Change file owner
		self._set_perms_for_txfr( fn_src )

		# Initiate transfer
		try:
			t = transfer.Transfer.start_new(
				self.isp.backup_endpoint, #src_endpoint
				self.isp.lts_endpoint,    #dst_endpoint
				fn_src,
				fn_dst,
				basepath
				)
		except ( RestartableTransferError ) as e:
			ev = events.StartTransferEvent( basepath )
			self.evMgr.delayPost( ev )
		else:
			# Save transfer info
			self._save_transfer( t )
			# Update backup_info file
			self._init_backup_host_info( bkup_info )
			bkup_info.update( {
				"status": TRANSFER_STARTED,
				"transfer_task_id": t.task_id,
				"source_path": fn_src,
				"destination_path": fn_dst,
				} )
			# Update last_status_check manually since call to
			# _update_transfer might fail due to timing issue with
			# globus
			bkup_info.set_datetime( 'last_status_check', t.last_update )
			logging.info( "transfer started ({0}) ({1})".format( basepath, fn_src) )


	def _recover_txfr( self, status, basepath, bkup_info ):
		task_id = bkup_info.transfer_task_id
		if status in ( TRANSFER_STARTED ):
			t = transfer.Transfer.from_task_id( task_id, basepath )
			self._update_transfer( t )
			self._initiate_cleanup_if_complete( t )
		elif status in ( TRANSFER_FINISHED ):
			logging.debug( "..found status=txfr_finished in _recover_txfr; about to post TxfrCompletedEvent" )
			self.evMgr.post( events.TransferCompletedEvent( task_id, basepath ) )
		elif status in ( TRANSFER_FAILED ):
			self.evMgr.post( events.TransferFailedEvent( task_id, basepath ) )
		else:
			e = UserWarning( "Unknown status '{0}' in _recover_txfr for basepath '{1}'".format( status, basepath) )
			logging.critical( e )
			raise e


	def _init_backup_host_info( self, bkup_info ):
		bkup_info.update( { 
			"pid": self.isp.get_pid(),
			"host": self.isp.get_hostname(),
			} )


	def _save_transfer( self, t ):
		t_pickle_stream = pickle.dumps( t )
		self.transfer_info_file.set( t.task_id, t_pickle_stream )


	def _set_perms_for_txfr( self, fn_src ):
		uid = int( self.isp.backup_uid )
		topdir = self.isp.work_dir
		if not fn_src.startswith( topdir ):	
			raise UserWarning( "mover._set_perms_for_txfr: file '{0}' not based at topdir '{1}'".format( fn_src, topdir ) )
		dn = fn_src.replace( topdir, "", 1 )
		while len( dn ) > 1:
			os.chown( topdir + dn, int( self.isp.backup_uid ), -1 )
			dn = os.path.dirname( dn )



	def cleanup_transfer( self, ev ):
		logging.debug( ">>>Enter" )

		task_id = ev.task_id
		basepath = ev.path
		bkup_info_fn = self.isp.mk_absolute_path_backup_info( basepath )
		archdir = self.isp.mk_absolute_path( "archive_dir", basepath )
		try:
			self.isp.acquireDirLock( archdir )
		except ( LockFileExists, LockFileBusy ) as e:
			# Another clean up attempt will happen at next
			# StartOfSemidayEvent via either 
			# check_open_transfers(if transfer is in transfer_info_file)
			# or StartBackupEvent or StartTransferEvent (all depending
			# on current backup_info status)
			#logging.debug( "..Could not acquire lock for dir '{0}', Caught exception '{1}': no files are changed, exiting this function now with the expectation that cleanup will happen again at next StartOfSemidayEvent".format( e ) )
			#TODO FIXME runtime crash when trying to format( e ) ...
			# ... RuntimeError: maximum recursion depth exceeded while getting the str of an object
			logging.debug( "..Could not acquire lock for dir '{0}': no files are changed, exiting this function now with the expectation that cleanup will happen again at next StartOfSemidayEvent".format( archdir ) )
			return

		caught_err = None
		status = None
		reason = None
		new_event = None
		if os.path.exists( bkup_info_fn ):
			bkup_info = infofile.InfoFile( bkup_info_fn )
			try:
				t = transfer.Transfer.from_task_id( task_id, basepath )
			except ( TransferLoadError ) as e:
				caught_err = e
				reason = str( e )
			else:
				if t.is_success():
					status = TRANSFER_FINISHED
				elif t.is_error():
					status = TRANSFER_FAILED
					# TODO - does globus info provide a failure reason?
				else:
					status = TRANSFER_FAILED
					caught_err = TransferError( task_id, "unknown transfer status '{0}': transfer is done but reports neither success nor failure (label={1})".format( t.status, t.label ) )
			self._init_backup_host_info( bkup_info )
			if status is not None:
				bkup_info.set( "status", status )
			if caught_err is not None:
				logging.warning( caught_err )
				new_event = events.TransferFailedEvent( task_id, basepath )
				if reason is not None:
					bkup_info.set( "last_error", reason )
			else:
				new_event = events.BackupTransferCompletedEvent( basepath )
				try:
					sp = bkup_info.source_path
				except ( AttributeError ) as e:
					sp = "no source_path found in backup info file"
				logging.info( "transfer completed (basepath={0} taskid={1} source_path={2})".format( basepath, task_id, sp ) )
			self.evMgr.post( new_event )
		else:
			logging.debug( "..bkup_info_fn doesn't exist, assume txfr was already cleaned up for ({0} {1})".format( basepath, task_id ) )
			self.transfer_info_file.delete( task_id )

		#unlock directory
		self.isp.releaseDirLock( archdir )

		logging.debug( "..delete task from file ({0} {1})".format( task_id, basepath ) )
		self.transfer_info_file.delete( task_id )

		logging.debug( "<<<Exit" )

	def transfer_failed_handler( self, ev ):
		""" Lock dir, thus forcing external action to be taken before
			any further processing is attempted.
		"""
		logging.debug( ">>>Enter" )
		# Try to lock dir
		basepath = ev.path
		archdir = self.isp.mk_absolute_path( "archive_dir", basepath )
		try:
			self.isp.acquireDirLock( archdir )
		except ( LockFileExists, LockFileBusy ):
			pass
		logging.debug( "<<<Exit" )


if __name__ == "__main__":
	isp = serviceprovider.ServiceProvider()
	isp.loadConfig( os.path.join( os.getcwd(), "conf/lustre_backup.cfg" ) )
	mvr = Mover()

#	###
#	# Load existing transfers from globusonline
#	###
#	for t_id in [ 
#		"05f7ba82-4aba-11e2-b73c-12313906b091", 
#		"fd4d85a6-4ab9-11e2-b73c-12313906b091" 
#		]:
#		t = transfer.Transfer.from_id( t_id )
#		mvr._save_transfer( t )
#		print( "Saved transfer: {0}".format( t ) )

#	###
#	# Initiate new globusonline transfer (as user backup)
#	###
#	src_fn = "/scratch/backup/work/A10GFile"
#	dst_fn = src_fn.replace( isp.work_dir, isp.lts_basedir, 1 )
#	t = transfer.Transfer.start_new(
#		src_endpoint=isp.backup_endpoint,
#		dst_endpoint=isp.lts_endpoint,
#		src_filename=src_fn,
#		dst_filename=dst_fn )
#	print( "Initiated transfer: {0}".format( t ) )
#	mvr._save_transfer( t )
#	print( "Saved transfer: {0}".format( t ) )
#	print( "Transfer status:\n{0}".format( pprint.pformat( t.info ) ) )

#	###
#	# Load / check transfers stored in local state file
#	###
	mvr.check_open_transfers( events.StartOfSemidayEvent() )

#	###
#	# Test start_new_transfer
#	###
#	users = [ "wkramer", "mdk", "gshi" ]
#	for uname in users:
#		basepath = "/u/staff/" + uname
#		ev = events.StartTransferEvent( basepath )
#		print "{0}".format( ev )
#		mvr.start_new_transfer( ev )
