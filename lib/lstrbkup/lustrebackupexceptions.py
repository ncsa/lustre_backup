class LustreBackupError( Exception ): pass

class FileError( Exception ):
	def __init__( self, filename=None, *a, **k ):
		self.filename = filename
	def __repr__( self ):
		return "<{0} ({1})>".format( self.__class__.__name__, self.filename )
	__str__ = __repr__

class ReadFileError( FileError ): pass

class OpenFileError( FileError ): pass

class CreateFileError( FileError ): pass

class ZeroLengthFileError( FileError ): pass

class LockFileError( FileError ): pass

class LockFileExists( LockFileError ): pass

class LockFileBusy( LockFileError ): pass

class LockFileIntegrityError( LockFileError ): pass

class LockFileHostPidMismatch( LockFileError ): pass

class DarBackupError( LustreBackupError ):
	def __init__( self, basepath=None, *a, **k ):
		super( DarBackupError, self ).__init__( *a, **k )
		self.basepath = basepath
	def __repr__( self ):
		return "<{0} ({1})>".format( self.__class__.__name__, self.basepath )
	__str__ = __repr__

class BackupInProgress( DarBackupError ): pass

class BackupAlreadyCompleted( DarBackupError ): pass

class BackupBasepathDoesNotExist( DarBackupError ): pass

class CatalogInProgress( DarBackupError ): pass

class BackupFailed( DarBackupError ): pass

class CleanupFailed( DarBackupError ):
	def __init__( self, msg=None, *a, **k ):
		super( CleanupFailed, self ).__init__( *a, **k )
		self.msg = msg
	def __repr__( self ):
		return "<{0}, (basepath={1}, msg={2})>".format( self.__class__.__name__, self.basepath, self.msg )
	__str__ = __repr__

#TODO - Should this also have basepath included?
class TransferError( LustreBackupError ):
	def __init__( self, task_id=None, msg=None, *a, **k ):
		super( TransferError, self ).__init__( *a, **k )
		self.task_id = task_id
		self.msg = msg
	def __repr__( self ):
		return "<{0} ({1}) {2}>".format( self.__class__.__name__, self.task_id, self.msg )
	__str__ = __repr__

class RestartableTransferError( TransferError ): pass

class NonRestartableTransferError( TransferError ): pass

class TransferLoadError( TransferError ): pass

class HungProcessError( LustreBackupError ):
	def __init__( self, hostname=None, pid=None, msg=None, *a, **k ):
		super( HungProcessError, self ).__init__( *a, **k )
		self.hostname = hostname
		self.pid = pid
		self.msg = msg
	def __repr__( self ):
		return "<{0} (host={1} pid={2} msg={3})>".format(
			self.__class__.__name__, 
			self.hostname,
			self.pid,
			self.msg )
	__str__ = __repr__


class GlobusError( LustreBackupError ):
	def __init__( self, msg, code, reason, *a, **k ):
		super( GlobusError, self ).__init__( *a, **k )
		self.msg = msg
		self.code = code
		self.reason = reason
	def __repr__( self ):
		return "<{0} (MSG:{1}) (code='{2}', reason='{3}')>".format(
			self.__class__.__name__, self.msg, self.code, self.reason )
	__str__ = __repr__

class NonFatalGlobusError( GlobusError ): pass

class FatalGlobusError( GlobusError ): pass
