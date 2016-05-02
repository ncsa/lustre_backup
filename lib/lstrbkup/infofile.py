import ConfigParser
import datetime
import os
import os.path

#local imports
from lustrebackupexceptions import *

class InfoFile( object ):
	""" Interface with backup.info status file.
		Wraps ConfigParser to work like a dict (by making all data
		part of a single section.
	"""

	section_name = "GENERAL"
	DATE_PATTERN = "%Y%m%d_%H%M%S"

	def __init__( self, filename, create_ok=False ):
		self.filename = filename
		self.create_ok = create_ok


	def as_dict( self ):
		return dict( self.cfg.items( self.section_name ) )
#		try:
#			d = dict( self.cfg.items( self.section_name ) )
#		except ( ConfigParser.NoSectionError ) as e:
#			raise FileError( self.filename, str( e ) )
#		return d


	def set( self, k, v ):
		""" Set a single key - value pair.
		"""
		self.update( { k: v } )


	def update( self, data={} ):
		""" Set many key - value pairs.
		"""
		for (k,v) in data.iteritems():
			self.cfg.set( self.section_name, k, str( v ) )
		self._save()


	def get_datetime( self, k ):
		""" Get value for key and automatically convert it to a Python
			native datetime object.  Note: this assumes that the value
			was stored using set_datetime and thus is already formatted
			properly.
		"""
		v = getattr( self, k )
		return datetime.datetime.strptime( v, self.DATE_PATTERN )


	def set_datetime( self, k, v ):
		""" Store a datetime in the infofile.  Automatically converts
			the native Python datetime object into a standard string
			representation for storing in the file.
			INPUT: k - (string) key for this value
				   v - (datetime) Python datetime object
		"""
		str_val = v.strftime( self.DATE_PATTERN )
		self.set( k, str_val )


	def delete( self, k ):
		self.cfg.remove_option( self.section_name, k )
		self._save()


	def getmtime( self ):
		return datetime.datetime.fromtimestamp( 
			os.path.getmtime( self.filename ) )

	def _load( self ):
		""" Read contents of (or create) the ConfigParser file.
			To allow lazy loading, this must return the cfg obj.
		"""
		cfg = ConfigParser.RawConfigParser()
		cfg.optionxform = str #case sensitive option names
		ok_files = cfg.read( self.filename )
		if self.filename not in ok_files:
			if not self.create_ok:
				raise ReadFileError( self.filename )
			cfg.add_section( self.section_name )
			self._save_cfg( cfg )
		self.cfg = cfg
		return cfg

	
	def _save_cfg( self, cfg ):
		""" Need this separate save function necessary for lazy loading.
		"""
		try:
			f = open( self.filename, 'wb' )
		except ( IOError ) as e:
			raise OpenFileError( filename=self.filename )
		#with open( self.filename, 'wb' ) as f:
		cfg.write( f )
		f.flush()
		os.fsync( f.fileno() )
		f.close()


	def _save( self ):
		""" Convenience function that calls _save_cfg.
		"""
		self._save_cfg( self.cfg )


	def rename_auto_date_sfx( self, reason=None, pfx="" ):
		""" Similar to rename but uses live timestamp as suffix.
		"""
		copy_date = datetime.datetime.now().strftime( "%Y%m%d_%H%M%S" )
		return self.rename( reason=reason, pfx=pfx, sfx=copy_date )


	def rename( self, reason=None, pfx="", sfx="", sep="." ):
		rename_src = self.filename
		( old_dn, old_fn ) = os.path.split( rename_src )
		if len( pfx ) > 0:
			pfx = pfx + sep
		if len( sfx ) > 0:
			sfx = sep + sfx
		new_fn = "{p}{fn}{s}".format( p=pfx, fn=old_fn, s=sfx )
		rename_dest = os.path.join( old_dn, new_fn )
		os.rename( rename_src, rename_dest )
		self.filename = rename_dest
		self._load()
		self.update( { "rename_reason": reason } )


	def __getattr__( self, name ):
		if name == "cfg":
			return self._load()
		rv = None
		try:
			rv = self.cfg.get( self.section_name, name )
		except ( ConfigParser.NoSectionError, ConfigParser.NoOptionError ) as e:
			raise FileError( self.filename, str( e ) )
		return rv
