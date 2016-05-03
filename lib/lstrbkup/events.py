import dar

#SECURITY NOTE: anything in here can be created simply by sending the 
# class name over the network.  This is a potential vulnerability
# I wouldn't suggest letting any of these classes DO anything, especially
# things like file system access, or allocating huge amounts of memory

class Event( object ):
	"""A superclass for any events that might be generated by an
	object and sent to the EventManager"""
	def __init__( self, *a, **k ):
		pass
	def __str__( self ):
		return self.__class__.__name__

#---------------------------------------------------------------#
# EVENTS 
#---------------------------------------------------------------#

class TickEvent( Event ): pass

class StartOfDayEvent( Event ): pass
class StartOfSemidayEvent( Event ): pass
class StartOfSemihourEvent( Event ): pass

#---------------------------------------------------------------#
# FILESYSTEM EVENTS 
#---------------------------------------------------------------#

class FilesystemEvent( Event ):
	def __init__( self, topdir, *a, **k ):
		super( FilesystemEvent, self).__init__( *a, **k )
		self.topdir = topdir
	def __str__( self ):
		return self.__repr__()
	def __repr__( self ):
		return "<{0} ({1})>".format( self.__class__.__name__, self.topdir )

class ScanForNewDirsEvent( FilesystemEvent ): pass
class ScanForFullBackupsEvent( FilesystemEvent ): pass
class ScanForIncrementalBackupsEvent( FilesystemEvent ): pass
class NoMoreFullBackupsEvent( FilesystemEvent ): pass

#---------------------------------------------------------------#
# BASEPATH EVENTS 
#---------------------------------------------------------------#

class BasepathEvent( Event ):
	def __init__( self, path, *a, **k ):
		super( BasepathEvent, self).__init__( *a, **k )
		self.path = path
	def __str__( self ):
		return self.__repr__()
	def __repr__( self ):
		return "<{0} ({1})>".format( self.__class__.__name__, self.path )

class NewBasepathEvent( BasepathEvent ): pass
class DeletedBasepathEvent( BasepathEvent ): pass

#---------------------------------------------------------------#
# BACKUP EVENTS 
#---------------------------------------------------------------#
class StartFullBackupEvent( BasepathEvent ):
	def __init__( self, *a, **k ):
		super(StartFullBackupEvent, self).__init__( *a, **k )
		self.backupType = dar.FULL

class StartIncrementalBackupEvent( BasepathEvent ):
	def __init__( self, *a, **k ):
		super(StartIncrementalBackupEvent, self).__init__( *a, **k )
		self.backupType = dar.INCREMENTAL

class BackupCreateArchiveCompletedEvent( BasepathEvent ): pass
class BackupCompletedEvent( BasepathEvent ): pass
class StartTransferEvent( BasepathEvent ): pass
class BackupTransferCompletedEvent( BasepathEvent ): pass
class StartCleanupSuccessfulBackupEvent( BasepathEvent ): pass
class CleanupSuccessfulBackupCompletedEvent( BasepathEvent ): pass

#---------------------------------------------------------------#
# TRANSFER EVENTS
#---------------------------------------------------------------#
class TransferEvent( BasepathEvent ):
	def __init__( self, task_id, *a, **k ):
		super( TransferEvent, self).__init__( *a, **k )
		self.task_id = task_id
	def __str__( self ):
		return self.__repr__()
	def __repr__( self ):
		return "<{0} ({1} {2})>".format( self.__class__.__name__, self.task_id, self.path )

class TransferCompletedEvent( TransferEvent ): pass
class TransferFailedEvent( TransferEvent ): pass

#---------------------------------------------------------------#
# REMOTEHOST EVENTS
#---------------------------------------------------------------#
class RemoteHostEvent( Event ):
	def __init__( self, remoteHostID, *a, **k ):
		super( RemoteHostEvent, self).__init__( *a, **k )
		self.remoteHostID = remoteHostID
	def __str__( self ):
		return self.__repr__()
	def __repr__( self ):
		return "<{0} ({1})>".format( self.__class__.__name__, self.remoteHostID )

class RemoteHostCompletedEvent( RemoteHostEvent ): pass
class RemoteHostUpEvent( RemoteHostEvent ): pass

#---------------------------------------------------------------#
# WARNINGS
#---------------------------------------------------------------#

class GenericWarningEvent( Event ): pass

#class DuplicateBasepathWarningEvent( GenericWarningEvent ):
#	def __init__( self, path, err, *a, **k ):
#		super( DuplicateBasepathWarningEvent, self ).__init__( *a, **k )
#		self.path = path
#		self.err = err
#	def __repr__( self ):
#		return "<{0} ({1}) {2}>".format( self.__class__.__name__, self.path, self.err )