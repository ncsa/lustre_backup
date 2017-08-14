import versioncheck
import datetime
import os.path
import string
import globusonlineconnection
import logging
from lustrebackupexceptions import *


class Transfer( object ):
	# Prevent multiple calls to globus in quick succession
	update_threshold = datetime.timedelta( seconds=60 )

	def __init__( self, info={} ):
		self.go = globusonlineconnection.GlobusOnlineConnection()
		self.info = info
		self.basepath = None
		self.last_update = datetime.datetime.min
		self.submission_id = None
		if "task_id" not in self.info:
			self.info[ "task_id" ] = None


	@classmethod
	def start_new( cls, src_endpoint, dst_endpoint, src_filename,
		dst_filename, basepath, label=None ):
		self = cls()
		if self.submission_id is None:
			self._get_new_id()
		self.basepath = basepath
		if label is None:
			label = self._mk_label( src_filename, basepath )
		err = None
		try:
			self.info = self.go.start_new_transfer( self.submission_id, 
				src_endpoint, src_filename, dst_endpoint, dst_filename, 
				label=label, verify_checksum=True )
		except ( NonFatalGlobusError ) as e:
			logging.warning( e )
			err = RestartableTransferError( task_id=None, msg=str( e ) )
		except ( FatalGlobusError ) as e:
			err = NonRestartableTransferError( task_id=None, msg=str( e ) )
			logging.error( err )
		if err is not None:
			raise err
		self.last_update = datetime.datetime.now()
		return self

	@classmethod
	def from_task_id( cls, task_id, basepath ):
		self = cls( { "task_id": task_id } )
		self._load()
		self.basepath = basepath
		return self

	def is_done( self ):
		self.update()
		return self.status in ( "SUCCEEDED", "FAILED" )

	def is_success( self ):
		self.update()
		return self.status in ( "SUCCEEDED" )

	def is_error( self ):
		self.update()
		return self.status in ( "FAILED" )

	def update( self, force=False ):
		if not force:
			elapsed = datetime.datetime.now() - self.last_update
			if elapsed < Transfer.update_threshold:
				return
		self._load()


	def _load( self ):
		""" Retrieve latest info from globusonline
		"""
		try:
			self.info = self.go.get_transfer_details( self.task_id )
		except ( FatalGlobusError ) as e:
			if self.info.history_deleted == "True":
				logging.warning( "globus task history was deleted for task_id='{0}'".format( self.task_id ) )
			err = TransferLoadError( task_id=self.task_id, msg=str( e ) )
			logging.error( err )
			raise err
#		try:
#			subtask_details = self.go.get_subtask_details( self.task_id )
#		except ( NonFatalGlobusError ) as e:
#			#DO SOMETHING
#		try:
#			subtask = subtask_details[ "DATA" ][ 0 ]
#		except( IndexError ) as e:
#			msg = "No subtask_details returned."
#			err = TransferLoadError( task_id=self.task_id, msg=msg )
#			logging.error( err )
#			raise err
#		self.info[ "subtask" ] = subtask
#		for key in ( "source_path", "destination_path" ):
#			self.info[ key ] = subtask[ key ]
		self.last_update = datetime.datetime.now()


	def _get_new_id( self ):
		if self.submission_id is None:
			self.submission_id = self.go.submission_id()
		return self.submission_id

	def _mk_label( self, fn_src, basepath=None ):
		(dn, fn) = os.path.split( fn_src )
		#make username part (path and user)
		userpath_tr_tbl = string.maketrans( "/.", "__" )
		delchars = "'\""
		if basepath:
			userpath = basepath
		else:
			( dn, d1 ) = os.path.split( dn )
			( dn, d2 ) = os.path.split( dn )
			( dn, d3 ) = os.path.split( dn )
			userpath = "_".join( [ d3, d2, d1 ] )
		userpath = userpath.translate( userpath_tr_tbl, delchars )
		#make filename part (date and backup_type)
		fn_tr_tbl = string.maketrans( "/.", "  " )
		fn = fn.translate( fn_tr_tbl, delchars ).split()[0]
		label = "{0} {1}".format( userpath, fn )
		return label

	def __getstate__( self ):
		""" How to pickle a Transfer.
		"""
		odict = self.__dict__.copy()
		del odict[ "go" ]
		return odict

	def __setstate__( self, odict ):
		""" How to un-pickle a Transfer.
		"""
		self.__dict__.update( odict )
		self.go = globusonlineconnection.GlobusOnlineConnection()

	def __getattr__( self, name ):
		#TODO - maybe change this to convert value to appropriate
		#		native type
		# NOTE: Access attempt will throw exception if name DNE so don't
		# have to check manually
		return self.info[ name ]

	def __str__( self ):
		try:
			status = self.status
		except KeyError:
			status = None
		return( "<{0} {1} {2}>".format( 
			self.__class__.__name__, 
			self.task_id,
			status ) )


if __name__ == "__main__":
	import pprint
	import serviceprovider
	import pickle
	isp = serviceprovider.ServiceProvider()
	isp.loadConfig( "lustre_backup.cfg" )
	print( "{0}\n>>>LIVE STATUS\n{0}".format( '-'*15 ) )
	# a failed transfer
	task_id = "202cb664-825f-11e2-b74b-12313906b091"
	task_basepath = "/u/sciteam/bernardi"
	#task_id = "14066654-9bec-11e2-97ce-123139404f2e"
	#task_basepath = "/u/sciteam/virgus"
	t = Transfer.from_task_id( task_id, task_basepath )
	pprint.pprint( t )
	pprint.pprint( t.info )

#	print( "{0}\n>>>SAVE TO FILE\n{0}".format( '-'*15 ) )
#	t = Transfer.from_task_id( "fd4d85a6-4ab9-11e2-b73c-12313906b091" )
#	t_ps = pickle.dumps( t )
#	with open( 'transfer_pickle_stream_file', 'w' ) as f:
#		f.write( t_ps )
#	print( "OK" )

#	print( "{0}\n>>>LOAD FROM FILE\n{0}".format( '-'*15 ) )
#	with open( 'transfer_pickle_stream_file' ) as f:
#		pickle_stream = f.read()
#	t = pickle.loads( pickle_stream )
#	pprint.pprint( t )
#	pprint.pprint( t.info )
#	pprint.pprint( { "BEFORE UPDATE": True, "LAST_UPDATE": t.last_update.strftime("%Y%m%d.%H%M%S") } )
#	t._load()
#	pprint.pprint( { "NOW": datetime.datetime.now().strftime("%Y%m%d.%H%M%S"), "LAST_UPDATE": t.last_update.strftime("%Y%m%d.%H%M%S") } )
#	print( "OK" )
