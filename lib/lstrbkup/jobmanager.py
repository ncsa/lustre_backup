from collections import deque
import rpyc
import eventmanager
import serviceprovider
import events
import socket
#import datetime
import logging

#---------------------------------------------------------------#
# JobManager
#---------------------------------------------------------------#

class JobManager( object ):
	def __init__( self ):
		self.evMgr = eventmanager.EventManager()
		self.isp = serviceprovider.ServiceProvider()
		self.remote_hostnames = []
		self.busyHosts = deque()
		self.freeHosts = deque()
		self.downHosts = deque()
		self.errHosts = deque()
		self.allHosts = {}
		# Register for all relevant events
		self.evMgr.registerListener( {
			'events.TickEvent': self.handleTickEvent,
			'events.StartOfSemidayEvent': self.handleStartOfSemidayEvent,
			'events.StartFullBackupEvent': self.startBackup,
			'events.StartIncrementalBackupEvent': self.startBackup,
			'events.ScanForNewDirsEvent': self.scheduleFilesystemScan,
			'events.ScanForFullBackupsEvent': self.scheduleFilesystemScan,
			'events.ScanForIncrementalBackupsEvent': self.scheduleFilesystemScan,
			'events.StartCleanupSuccessfulBackupEvent': self.startCleanupSuccessfulBackup,
			'events.RemoteHostCompletedEvent': self.cleanupRemoteServer,
			'events.RemoteHostUpEvent': self.activateHost,
			} )
		self._initialize_hosts()
		for (k,v) in self.allHosts.iteritems():
			logging.debug( "HOSTNAME-ID-MAP: {0:>s} {1:2d}".format( str(v), k ) )

	def scheduleFilesystemScan( self, ev ):
		""" Handler for Scan* FilesystemEvent's.
		"""
		# Find remote server
		try:
			remote_server = self.nextHost()
		except NoHostAvailableError:
			self.evMgr.delayPost( ev )
			return
		# Create remote filesystem scanner
		remote_scanner = remote_server.conn.root.RemoteFilesystemScanner( \
			ev.topdir, \
			self.handleRemoteEvent, \
			remote_server.id )
		remote_server.setInstance( remote_scanner )
		# Event class to remote method name map
		ev_mn = {
			events.ScanForNewDirsEvent: "startScanForNewAndDeletedDirs",
			events.ScanForFullBackupsEvent: "startScanForFullBackupsNeeded",
			events.ScanForIncrementalBackupsEvent: "startScanForIncrementalBackupsNeeded",
		}
		methodName = ev_mn[ ev.__class__ ]
		remote_server.setCallable( methodName )
		remote_server.startRemoteAction()

	def handleTickEvent( self, ev ):
		self.checkBusyHosts()
		self.ls_hosts()

	def ls_hosts( self ):
		logging.debug( "busyHosts: {0}".format( " ".join( [ str(h) for h in self.busyHosts ] ) ) )
		logging.debug( "freeHosts: {0}".format( " ".join( [ str(h) for h in self.freeHosts ] ) ) )
		logging.debug( "downHosts: {0}".format( " ".join( [ str(h) for h in self.downHosts ] ) ) )
		logging.debug( "errHosts: {0}".format( " ".join( [ str(h) for h in self.errHosts ] ) ) )
			

	def handleStartOfSemidayEvent( self, ev ):
		self.checkDownHosts()

	#TODO: If a remote host throws an error, this is the only way it
	#      will get processed.  Don't just blindly generate
	#      RemoteHostCompletedEvent because there may already be one
	#      on the eventQueue waiting to be processed.
	def checkBusyHosts( self ):
		""" Check busy hosts for unhandled errors
		"""
		for h in self.busyHosts:
			if h.isDone() and h.isError():
				self.evMgr.post( events.RemoteHostCompletedEvent( h.id ) )

	def checkDownHosts( self ):
		""" Check down hosts to see if they are online
		"""
		pass
#		for h in self.downHosts:
#			try:
#				conn = h.connect()
#			except RemoteHostDownError:
#				pass
#			else:
#				self.evMgr.post( RemoteHostUpEvent( h.id ) )

	def activateHost( self, ev ):
		h = self.allHosts[ ev.remoteHostID ]
		self.downHosts.remove( h )
		self.freeHosts.append( h )

	def cleanupRemoteServer( self, ev ):
		remoteHost = self.allHosts[ ev.remoteHostID ]
		remoteHost.close()
		self.busyHosts.remove( remoteHost )
		if remoteHost.isError():
#			self.errHosts.append( remoteHost )
			logging.warning( 
				"remote host error: hostname='{0}', action='{1}', error='{2}'".format( 
					remoteHost, 
					remoteHost.remote_function_name,
					remoteHost.lastError ) )
#		else:
#			self.freeHosts.append( remoteHost )
		self.freeHosts.append( remoteHost )


	def handleRemoteEvent( self, cls_name, *args ):
		""" Receive events from remote servers.
			Pass this method as a callback to remote async processes.
		"""
		event_class = getattr( events, cls_name )
		new_event = event_class( *args )
		logging.debug( "about to post remote event ({0})".format( new_event ) )
		self.evMgr.post( new_event )


	def nextHost( self ):
		""" Find an available remote server that is online."""
		if len( self.freeHosts ) < 1:
			raise NoHostAvailableError()
		conn = None
		while conn is None and len( self.freeHosts ) > 0:
			h = self.freeHosts.popleft()
			try:
				conn = h.connect()
			except RemoteHostDownError:
				self.downHosts.append( h )
				conn = None
		if conn is None:
			raise NoHostAvailableError()
		self.busyHosts.append( h )
		return h


	def _initialize_hosts( self ):
		self.remote_hostnames = self.isp.getHostnames()
		count = 0
		for (hn, ip) in self.remote_hostnames:
			count += 1
			self.allHosts[ count ] = \
				RemoteHost( hn, ip, self.isp.remote_port, count ) 
			self.freeHosts.append( self.allHosts[ count ] )


	def startBackup( self, ev ):
		""" Handler for StartBackupEvent."""
		#DEBUG
		logging.debug( ">>>Enter" )
		if not self.isp.isBackupTypeAllowed( ev.backupType ):
			self.evMgr.delayPost( ev )
			return
		# Find remote server
		try:
			remote_server = self.nextHost()
		except NoHostAvailableError:
			self.evMgr.delayPost( ev )
			logging.debug( "..no host available" )
			logging.debug( "<<<Exit" )
			return
		# Create remote backup object
		remoteBU = remote_server.conn.root.RemoteLustreBackup( \
			ev.path, \
			ev.backupType, \
			self.handleRemoteEvent, \
			remote_server.id )
		remote_server.setInstance( remoteBU )
		remote_server.setCallable( "startBackup" )
		remote_server.startRemoteAction()
		logging.debug( "<<<Exit" )


	def startCleanupSuccessfulBackup( self, ev ):
		logging.debug( ">>>Enter" )
		# Find remote server
		try:
			remote_server = self.nextHost()
		except NoHostAvailableError:
			self.evMgr.delayPost( ev )
			logging.debug( "..no host available" )
			logging.debug( "<<<Exit" )
			return
		remoteBU = remote_server.conn.root.RemoteLustreBackup( \
			basepath=ev.path, \
			type=None,
			callback=self.handleRemoteEvent, \
			remoteHostID=remote_server.id )
		remote_server.setInstance( remoteBU )
		remote_server.setCallable( "startCleanup" )
		remote_server.startRemoteAction()
		logging.debug( "<<<Exit" )

#---------------------------------------------------------------#
# RemoteHost
#---------------------------------------------------------------#

class RemoteHost( object ):
	def __init__( self, hostname, ipaddr, port, id ):
		self.name = hostname
		self.ipaddr = ipaddr
		self.port = int( port )
		self.id = id
		self.conn = None
		self.remoteInst = None
		self.bgThread = None
		self.asyncCallable = None
		self.asyncResult = None
		self.lastError = None
		self.isActive = False

	def connect( self ):
		""" Try to connect to server.  If successfull, create a
			BgServingThread for callbacks.
		"""
		try:
			self.conn = rpyc.connect( self.ipaddr, self.port )
		except socket.error as e:
			self.conn = None
			self.lastError = e
			raise RemoteHostDownError()
		# Clear any previous error upon new, successful connection
		self.lastError = None
		self.bgThread = rpyc.BgServingThread( self.conn )
		return self.conn

	def setInstance( self, obj ):
		self.remoteInst = obj

	def setCallable( self, functionName ):
		#SYNCHRONOUS call - DEBUG
		#self.asyncCallable = getattr( self.remoteInst, functionName )

		#ASYNCHRONOUS call - for production
		remote_func = getattr( self.remoteInst, functionName )
		self.asyncCallable = rpyc.async( remote_func )
		self.remote_function_name = functionName
		self.remote_func = remote_func

	def startRemoteAction( self ):
		if self.asyncCallable is None:
			logging.error( "RemoteHost.startRemoteAction attempted but asyncCallable not defined" )
		logging.debug( "About to call {0} on host {1}".format( str( self.remote_func ), self.name ) )
		self.asyncResult = self.asyncCallable()
		self.isActive = True

	def isDone( self ):
		rv = True
		if self.isActive:
			rv = self.asyncResult.ready
		return rv

	def isError( self ):
		rv = self.lastError
		if self.isActive:
			rv = self.asyncResult.error
		return rv

	def isPingable( self ):
		rv = False
		if self.conn:
			rv = True
		else:
			self.connect()
			self.close()
			rv = True
		return rv

	def close( self ):
		logging.debug( ">>>Enter" )
		logging.debug( "..host '{0}'".format( self ) )
		if self.conn.closed:
			e = UserWarning( "Remote host error ... attempt to close an already \
closed connection, host='{0}', action='{1}'. Is this a \
duplicate or out of order event?".format(
				self.name,
				self.remote_function_name ) )
			logging.error( e )
			# Assume cleanup has already been done, so just return
			return
		else:
			logging.debug( "..remote_func: {0}".format( str( self.remote_function_name ) ) )
		###
		# Seems to make more sense to check for errors while
		# connection is open, then close it (instead of the other way
		# around).
		###
#		self.bgThread.stop()
#		if not self.conn.closed:
#			self.conn.close()
		if self.asyncResult is not None:
			if not self.asyncResult.ready:
				self.asyncResult.set_expiry( 5 )
			try:
				res = self.asyncResult.value
			except Exception as e:
				logging.debug( "..asyncResult caught error: {0}".format( e ) )
				self.lastError = e
			else:
				logging.debug( "..asyncResult returned value {0}".format( res ) )
		else:
			logging.debug( "..asyncResult is None" )
		###
		# Thought this would be good here, but maybe it is what is causing the
		# blocked threads, so moving it back up above.
		###
		self.bgThread.stop()
		if not self.conn.closed:
			self.conn.close()

		self.asyncCallable = None
		self.asyncResult = None
		self.isActive = False
		logging.debug( "<<<Exit" )

	def __str__( self ):
		return self.name
	
	def __repr__( self ):
		return "<{0} ({1})({2})>".format(self.__class__, self.name, self.id)

#---------------------------------------------------------------#
# Errors
#---------------------------------------------------------------#

class NoHostAvailableError( Exception ): pass
class RemoteHostDownError( Exception ): pass

if __name__ == '__main__':
	raise UserWarning( "JobManager cannot be invoked directly" )
