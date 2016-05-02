import eventmanager
import events
import serviceprovider
import dar
import logging

class Scheduler( object ):
	def __init__( self ):
		self.evMgr = eventmanager.EventManager()
		self.isp = serviceprovider.ServiceProvider()
		self.evMgr.registerListener( {
			'events.StartOfDayEvent': self.startOfDayHandler,
			'events.StartOfSemidayEvent': self.startOfSemidayHandler,
			'events.NewBasepathEvent': self.newBasepathHandler,
			'events.BackupCreateArchiveCompletedEvent': self.backupCreateArchiveCompletedHandler,
			'events.BackupTransferCompletedEvent': self.backupTransferCompletedHandler,
			'events.NoMoreFullBackupsEvent': self.enableIncrementalBackups,
			} )
		self.topdirs = {}
		for (k,v) in self.isp.get_topdirs():
			if v == "1":
				self.topdirs[ k ] = {}
		self._reset_topdirs()

	def startOfDayHandler( self, ev ):
		self.isp.disableIncrementalBackups()
		logging.info( "Incremental backups now disabled" )
		self._reset_topdirs()
		for topdir in self.topdirs.keys():
			self.evMgr.post( events.ScanForNewDirsEvent( topdir ) )

	def startOfSemidayHandler( self, ev ):
		for topdir in self.topdirs.keys():
			self.evMgr.post( events.ScanForFullBackupsEvent( topdir ) )
		if self.isp.isBackupTypeAllowed( dar.INCREMENTAL ):
			for topdir in self.topdirs.keys():
				self.evMgr.delayPost( events.ScanForIncrementalBackupsEvent( topdir ) )

	def newBasepathHandler( self, ev ):
		self.isp.setBasepathID( ev.path )
		self.evMgr.post( events.StartFullBackupEvent( ev.path ) )

	def enableIncrementalBackups( self, ev ):
		self.topdirs[ ev.topdir ][ "FULLS_DONE" ] = True
		if self._all_fulls_done():
			self.isp.enableIncrementalBackups()
			logging.info( "Incremental backups now enabled" )
			# Start incrementals immediately instead of waiting for next semiday event
			for topdir in self.topdirs.keys():
				self.evMgr.post( events.ScanForIncrementalBackupsEvent( topdir ) )

	def backupCreateArchiveCompletedHandler( self, ev ):
		self.evMgr.post( events.StartTransferEvent( ev.path ) )

	def backupTransferCompletedHandler( self, ev ):
		self.evMgr.post( events.StartCleanupSuccessfulBackupEvent( ev.path ) )

	def _all_fulls_done( self ):
		for ( topdir, d ) in self.topdirs.iteritems():
			logging.debug( "..{0}[FULLS_DONE]='{1}'".format( topdir, d[ "FULLS_DONE" ] ) )
			if not d[ "FULLS_DONE" ]:
				logging.debug( "..return False" )
				return False
		logging.debug( "..all fulls done, return True" )
		return True

	def _reset_topdirs( self ):
		for k in self.topdirs.keys():
			self.topdirs[ k ][ "FULLS_DONE" ] = False
