import os
import os.path
import datetime
import dar
import serviceprovider
import logging
import backup_dir

class FilesystemScanner( object ):
	def __init__( self, topdir ):
		self.topdir = topdir
		self.isp = serviceprovider.ServiceProvider()
		self.archTopDir = self.isp.mk_absolute_path( "archive_dir", self.topdir )
		self.skipDirs = [ "lost+found" ]
		self.now = datetime.datetime.now()
		self.cycle = int( self.isp.full_backup_frequency ) / 86400
		doy = self.now.timetuple().tm_yday
		self.periodic_full_mod_match = doy % self.cycle


	def scanForNewAndDeletedDirs( self ):
		""" Find New and Deleted dirs at basepath
			Return tuple (new_dir_list, deleted_dir_list)
		"""
		logging.debug( ">>>Enter ({0})".format( self.topdir ) )
		# get list of dirs in archive (static information) area
		arch_dirs_list = []
		arch_dir_contents = []
		try:
			arch_dir_contents = os.listdir( self.archTopDir )
		except OSError as e:
			if e.errno == 2:
				pass
			else:
				raise e
		for dn in arch_dir_contents:
			abs_dn = os.path.join( self.archTopDir, dn )
			if os.path.isdir( abs_dn ) \
			and not os.path.islink( abs_dn ) \
			and dn not in self.skipDirs:
				arch_dirs_list.append( dn )
		# get list of dirs on live filesystem
		active_dirs_list = []
		active_dirs_missing_id = []
		for dn in os.listdir(self.topdir):
			abs_dn = os.path.join( self.topdir, dn )
			if os.path.isdir( abs_dn ) \
			and not os.path.islink( abs_dn ) \
			and dn not in self.skipDirs:
				active_dirs_list.append( dn )
				# Verify that existing archdir has a valid id
				logging.debug( "Verify that existing archdir has a valid id..." )
				if dn in arch_dirs_list:
					basepath_id = self.isp.getBasepathID( abs_dn )
					if basepath_id is None:
						logging.debug( "..missing id for dir({0})".format( abs_dn ) )
						arch_dirs_list.remove( dn )
		active_dirs = set( active_dirs_list )
		arch_dirs = set( arch_dirs_list )
		new_dir_list = [ os.path.join( self.topdir, x ) for x in active_dirs - arch_dirs ]
		deleted_dir_list = [ os.path.join( self.topdir, x ) for x in arch_dirs - active_dirs ]
		logging.debug( "..archive_dirs='{0}'".format( arch_dirs ) )
		logging.debug( "..active_dirs='{0}'".format( active_dirs ) )
		logging.debug( "..new_dirs='{0}'".format( new_dir_list ) )
		logging.debug( "..deleted_dirs='{0}'".format( deleted_dir_list ) )
		logging.debug( ">>>Exit ({0})".format( self.topdir ) )
		return ( new_dir_list, deleted_dir_list )


	def scanForFullBackupsNeeded( self ):
		logging.debug( ">>>Enter ({0})".format( self.topdir ) )
		maxAge = int( self.isp.full_backup_frequency )
		all_fulls = self._scanForBackupNeeded( maxAge, bkup_type=dar.FULL )
		logging.debug( "..fulls needed = '{0}'".format( all_fulls ) )
		logging.debug( ">>>Exit ({0})".format( self.topdir ) )
		return all_fulls


	def scanForIncrementalBackupsNeeded( self ):
		logging.debug( ">>>Enter ({0})".format( self.topdir ) )
		maxAge = int( self.isp.incremental_backup_frequency )
		all_incrs = self._scanForBackupNeeded( maxAge, bkup_type=dar.INCR )
		logging.debug( "..incrs needed = '{0}'".format( all_incrs ) )
		logging.debug( ">>>Exit ({0})".format( self.topdir ) )
		return all_incrs


	def _scanForBackupNeeded( self, maxAge, bkup_type ):
		logging.debug( ">>>Enter ({0})".format( self.topdir ) )
		dirs_to_backup = []
		arch_dir_contents = []
		try:
			arch_dir_contents = os.listdir( self.archTopDir )
		except OSError as e:
			if e.errno == 2:
				pass
			else:
				raise e
		for dn in arch_dir_contents:
			abs_dn = os.path.join( self.archTopDir, dn )
			if dn in self.skipDirs:
				logging.warning( "..skipping hardset-skip dir ({0})".format( abs_dn ) )
				continue
			if not os.path.isdir( abs_dn ):
				logging.warning( "..skipping deleted dir ({0})".format( abs_dn ) )
				continue
			if self.isp.isDirLocked( abs_dn ):
				logging.warning( "..skipping locked dir ({0})".format( abs_dn ) )
				continue
			needs_backup = False
			basepath = os.path.join( self.topdir, dn )
			if not os.path.isdir( basepath ):
				logging.warning( "..skipping deleted dir ({0})".format( basepath ) )
				continue
			# Normal check for backup - using maxAge
			bu_dir = backup_dir.Backup_Dir( abs_dn )
			# TODO - resolve this issue
			# skip backups currently running so incrementals can
			# begin as soon as all fulls have at least started
			# but if skip backups in transfer state, potentially never
			# restart transfers if manager restarts and transfer info
			# file is restarted
			# ------
			# Is this actually an issue?  Backups in transfer state
			# are not locked.  If dir remains locked, then will
			# continually miss backups, then get manually fixed.  If
			# dir eventually unlocks, then Start-a-new-backup will
			# fast-forward to start a new transfer or check on existing 
			# transfer.
			# ------
			# See equivalent note in
			# backup_dir.py:is_backup_currently_running()
			if bu_dir.is_backup_currently_running():
				logging.debug( "..skipping dir with active backup ({0})".format( basepath ) )
				continue
			func = bu_dir.last_successful_backup
			if bkup_type == dar.FULL:
				func = bu_dir.last_full_backup
			logging.debug( 
				"..checking dir ({0}) for last backup with func ({1})...".format( 
					basepath, func ) )
			( last_bkup_date, last_bkup_info ) = func()
			if not last_bkup_date:
				last_bkup_date = datetime.datetime.min
			diff = self.now - last_bkup_date
			secs_since_last_backup = diff.days * 86400 + diff.seconds
			if secs_since_last_backup > maxAge:
				needs_backup = True
			# Check for periodic full - using basepathID mod cycle
			if bkup_type == dar.FULL:
				raw_id = self.isp.getBasepathID( basepath )
				logging.debug( "..check for periodic full needed (basepath={0}, id={1})".format( basepath, raw_id ) )
				if raw_id is not None:
					basepathID = int( raw_id )
					basepath_mod_match = basepathID % self.cycle
					if basepath_mod_match == self.periodic_full_mod_match \
					and secs_since_last_backup >= self.isp.incremental_backup_frequency :
						needs_backup = True
			if needs_backup:
				dirs_to_backup.append( basepath )
		logging.debug( "..dirs needing backup ({0})".format( dirs_to_backup) )
		logging.debug( ">>>Exit ({0})".format( self.topdir ) )
		return dirs_to_backup


if __name__ == "__main__":
	import pprint
	isp = serviceprovider.ServiceProvider()
	isp.loadConfig( "lustre_backup.cfg" )
	fss = FilesystemScanner( "/home" )
	for n in [ \
	"scanForNewAndDeletedDirs", \
	"scanForFullBackupsNeeded", \
	"scanForIncrementalBackupsNeeded" ]:
		f = getattr( fss, n )
		res = f()
		pprint.pprint( res )
