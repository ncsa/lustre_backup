
import versioncheck
import os
import os.path
import pprint
import optparse
import re
import datetime
import logging
logging.basicConfig(
	format="%(asctime)s.%(msecs)d - %(levelname)s - %(funcName)s[%(lineno)d] - %(message)s",
	datefmt="%H%M%S",
	level=logging.WARNING )
##Peewee logging
#logger = logging.getLogger('peewee')
#logger.setLevel(logging.DEBUG)
#logger.addHandler(logging.StreamHandler())

#local imports
from lstrbkup.stats_orm import *
from peewee import *
import lstrbkup.dar
import lstrbkup.mover

OPTS = None
DATEMIN = datetime.datetime( 1970, 1, 1 )
DATEOLD = datetime.datetime.now() - datetime.timedelta( days=7 )
RE_DATE_FMT = re.compile( "\d{8}_\d{6}" )

def process_options():
	parser = optparse.OptionParser()
	parser.add_option( "-d", "--dir", help="Dir to start scan.  [Default: %default]" )
	parser.add_option( "--debug", action="store_true" )
	parser.set_defaults( 
		dir="/u/system/backup/archive",
		debug=False,
		)
	( options, args ) = parser.parse_args()
	return ( options, args )


def get_db_basepaths():
	db = yabu_stats_db
	db.connect()
	sq = Backup.select( fn.Distinct( Backup.basepath ) )
	return [ record.basepath for record in sq ]


def scan_fs_basepaths():
	basepaths = []
	archlen = len( OPTS.dir )
	for ( root, dirs, files ) in os.walk( OPTS.dir ):
		# done if no more subdirs
		if len( dirs ) < 1:
			logging.debug( "Adding basepath, no more subdirs, {0}".format( root[archlen:] ) )
			basepaths.append( root[archlen:] )
		# done if subdirs match date format
		else:
			matches = 0
			nonmatches = 0
			for d in dirs:
				logging.debug( "..checking dir {0}/{1}".format( root, d ) )
				if RE_DATE_FMT.match( d ):
					matches = matches + 1
					dirs.remove( d )
				else:	
					nonmatches = nonmatches + 1
			logging.debug( "matches:{0} nonmatches:{1}".format( matches, nonmatches ) )
			if matches > 0 and nonmatches < 1:
				logging.debug( "Adding basepath, based on matches count, {0}".format( root[archlen:] ) )
				basepaths.append( root[archlen:] )
	return set( basepaths )
	

def get_db_last_successful_backups():
	db = yabu_stats_db
	db.connect()
	sq = Backup.select(
			Backup.basepath,
			Backup.type,
			Backup.status,
			Backup.date_time.alias( 'LastBkupDate' )
		).where( 
			Backup.status << [ lstrbkup.dar.DAR_COMPLETE, lstrbkup.dar.BACKUP_COMPLETE, lstrbkup.mover.TRANSFER_FINISHED ]
		).order_by( Backup.date_time.asc()
		).dicts()
	last_backup_data = {}
	for record in sq:
		last_backup_data[ record[ "basepath" ] ] = record
	return last_backup_data
	

#def do_query():
#	db = yabu_stats_db
#	db.connect()
#	data = {}
#	# get last bkup date and last bkup type
#	q1 = Backup.select(
#		Backup.basepath,
#		fn.Max( Backup.date_time ).alias( 'LastBkupDate' ),
#		Backup.type ).where(
#		Backup.status << [ lstrbkup.dar.DAR_COMPLETE, lstrbkup.dar.BACKUP_COMPLETE ] )
#	for bkup in q1.group_by( Backup.basepath ):
#		data[ bkup.basepath ] = {
#			'Basepath': bkup.basepath,
#			'LastBkupDate': bkup.LastBkupDate,
#			'LastBkupType': bkup.type
#		}
#	# get last full date
#	for bkup in q1.where( Backup.type=='FULL' ).group_by( Backup.basepath ) :
##		print bkup.basepath, bkup.LastBkupDate, bkup.type
#		data[ bkup.basepath ][ 'LastFullDate' ] = bkup.LastBkupDate
#	# get num FULL
#	q2 = Backup.select(
#		Backup.basepath,
#		Backup.type,
#		fn.Count( Backup.date_time ).alias( 'count' )
#	)
#	for bkup in q2.group_by( Backup.basepath, Backup.type ):
##		print bkup.basepath, bkup.type, bkup.count
#		key = "{0}count".format( bkup.type )
#		data[ bkup.basepath ][ key ] = bkup.count
#	return data
	

def add_status( data ):
	""" Add field "AcctStatus" to each data dictionary
		Return tuple( OK_data, DELETED_data )
	"""
	OK_accts = {}
	DELETED_accts = {}
	key = 'AcctStatus'
	for ( basepath, basepath_info ) in data.iteritems():
		#logging.debug( "checking acct_status of {0} with data {1}".format( basepath, pprint.pformat( basepath_info ) ) )
		if os.path.exists( basepath ):
			basepath_info[ key ] = 'OK'
			OK_accts[ basepath ] = basepath_info
		else:
			basepath_info[ key ] = 'DELETED'
			DELETED_accts[ basepath ] = basepath_info
	return ( OK_accts, DELETED_accts, )


def print_report( data, include_hdr=True ):
	sorted_data = sorted( data.items(), key=lambda x: x[1]["LastBkupDate"])

	date_fmt = "%Y-%m-%d %H:%M:%S"
#	fmt = "{LastBkupDate:19} {LastBkupType:8} {LastBkupStatus:15} {FULLcount:8} {INCRcount:8} {AcctStatus:>10} {Basepath}"
#	fmt = "{LastBkupDate:19} {type:^9} {status:15} {AcctStatus:>7} {basepath}"
	fmt = "{LastBkupDate:19}  {type:^9}  {status:15}  {basepath}"
	headers = {
		"LastBkupDate": "LAST-DATE",
		"type": "LAST-TYPE",
		"status": "LAST-STATUS",
#		"AcctStatus": "ACCOUNT",
		"basepath": "BASEPATH",
	}
	if include_hdr:
		print( fmt.format( **headers ) )
	date_keys_to_check = ( "LastBkupDate", )
	for ( basepath, basepath_info ) in sorted_data:
#		for int_key in int_keys_to_check:
#			if int_key not in basepath_info:
#				basepath_info[ int_key ] = 0
		for date_key in date_keys_to_check:
			basepath_info[date_key] = basepath_info[date_key].strftime( date_fmt )
		try:
			print( fmt.format( **basepath_info ) )
		except ( KeyError ) as e:
			logging.warning( "Caught Error: {0}\nData: {1}".format( e, pprint.pformat( [ basepath, basepath_info ] ) ) )


def check_old_bkups( acct_data ):
	pbkup_data = {}
	for ( basepath, bkup ) in acct_data.iteritems():
		last_bkup_date = bkup[ "LastBkupDate" ]
		if last_bkup_date < DATEOLD:
			logging.debug( "investigating OLD BACKUP of {0}".format( basepath ) )
			abspath = mk_archdir_path( basepath )
			pbkup_dates = find_pbkups( abspath )
			if len( pbkup_dates ) > 0:
				pbkup_data[ basepath ] = bkup
				pbkup_data[ basepath ][ "LastBkupDate" ] = pbkup_dates[0]
				pbkup_data[ basepath ][ "type" ] = "PARALLEL"
				pbkup_data[ basepath ][ "status" ] = "N/A"
	return pbkup_data


def mk_archdir_path( basepath ):
	clean_bp = basepath.lstrip( os.sep )
	return os.path.join( OPTS.dir, clean_bp )


def find_pbkups( abspath ):
	pbkup_dates = []
	logging.debug( "Checking dir for pbkups: {0}".format( abspath ) )
	for ( root, dirs, files ) in os.walk( abspath ):
		for name in dirs:
			dn = os.path.join( abspath, name )
#			logging.debug( "..checking {0}".format( dn ) )
			if RE_DATE_FMT.match( name ):
				logging.debug( ".. found pbkup: {0}".format( name ) )
				pbkup_dates.append( datetime.datetime.strptime( name, "%Y%m%d_%H%M%S" ) )
	return sorted( pbkup_dates, reverse=True )


#def get_last_pbkup_date( basepath ):
#	lastdate = DATEMIN
#	abspath = mk_archdir_path( basepath )
#	for name in os.listdir( abspath ):
#		fname = os.path.join( abspath, name )
#		if os.path.isdir( fname ):
#			try:
#				newdate = datetime.datetime.strptime( fname, "%Y%m%d_%H%M%S" )
#			except ( ValueError ) as e:
#				continue
#			if newdate > lastdate:
#				lastdate = newdate
#	return lastdate


if __name__ == "__main__":
	# Get cmdline options
	( OPTS, args ) = process_options()

	# setup logging
	db_logger = logging.getLogger( 'peewee' )
	db_logger.setLevel( logging.WARNING )
	db_logger.addHandler( logging.StreamHandler() )
	if OPTS.debug:
		db_logger.setLevel( logging.DEBUG )

	data = {}

	# Get basepaths from database
	for basepath in get_db_basepaths():
		data[ basepath ] = {
			"LastBkupDate": DATEMIN,
			"type": None,
			"status": None,
			"basepath": basepath,
			"src": "DB",
			}

	# Look for other basepaths in filesystem
	for basepath in scan_fs_basepaths():
		if basepath not in data:
			data[ basepath ] = {
				"LastBkupDate": DATEMIN,
				"type": None,
				"status": None,
				"basepath": basepath,
				"src": "DB",
				}

	# Populate last successful backup
	last_backup_data = get_db_last_successful_backups()
	data.update( last_backup_data )

	# Get account status
	( active_accts, del_accts ) = add_status( data )

	# Update PBKUP accounts
	pbkup_data = check_old_bkups( active_accts )
	active_accts.update( pbkup_data )

	# Display results
	print_report( active_accts )
	#print_report( del_accts, include_hdr=False )
