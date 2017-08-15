
import versioncheck
#import os.path
import pprint
import optparse
import logging
logging.basicConfig(
	format="%(asctime)s.%(msecs)d - %(levelname)s - %(funcName)s[%(lineno)d] - %(message)s",
	datefmt="%H%M%S",
	level=logging.WARNING )

#local imports
from lstrbkup.stats_orm import *
from peewee import *

OPTS = None

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

def do_query():
	db = yabu_stats_db
	db.connect()
	data = {}
	status_complete = ( 'backup_complete', 'txfr_finished' )
	# get last bkup date and last bkup type
	for bkup in Backup.select():
		start_date = bkup.date_time.date()
		if start_date not in data:
			data[ start_date ] = { 
				'FULL': { 'COUNT': 0, 'COMPLETED': 0 },
				'INCR': { 'COUNT': 0, 'COMPLETED': 0 }
			}
		typ = bkup.type
#		if typ not in data[ start_date ]:
#			data[ start_date ][ typ ] = { 'COUNT': 0, 'COMPLETED': 0 }
		data[ start_date ][ typ ][ 'COUNT' ] += 1
		if bkup.status in status_complete:
			data[ start_date ][ typ ][ 'COMPLETED' ] += 1
	return data

def print_report( data ):
	all_types = [ item for x in data.values() for item in x.keys() ]
	uniq_types = sorted( set( all_types ) )
	rows = []
	for startdate in sorted( data.iterkeys(), reverse=False ):
		vals = { 'DATE': str( startdate ) }
		#totals = { 'COUNT': 0, 'COMPLETED': 0, 'PCT_COMPLETED': 0 }
		# massage data into a single level dict called vals
		for typ in uniq_types:
			d = data[ startdate ][ typ ]
			for (k,v) in d.iteritems():
				key = "{0}_{1}".format( typ, k )
				vals[ key ] = v
		rows.append( vals )
	fmt = "{DATE:10s}  {FULL_COUNT:>10}  {FULL_COMPLETED:>14}  {INCR_COUNT:>10}  {INCR_COMPLETED:>14}"
	hdr = {
		"DATE": "DATE",
		"FULL_COUNT": "FULL_COUNT",
		"FULL_COMPLETED": "FULL_COMPLETED",
		"INCR_COUNT": "INCR_COUNT",
		"INCR_COMPLETED": "INCR_COMPLETED",
		}
	linecount = 0
	for r in rows:
		if linecount % 30 == 0:
			print( fmt.format( **hdr ) )
		print( fmt.format( **r ) )
		linecount += 1



if __name__ == "__main__":
	# Get cmdline options
	( OPTS, args ) = process_options()

	# setup logging
	db_logger = logging.getLogger( 'peewee' )
	db_logger.setLevel( logging.WARNING )
	db_logger.addHandler( logging.StreamHandler() )
	if OPTS.debug:
		db_logger.setLevel( logging.DEBUG )

	# Get database results
	data = do_query()

	#pprint.pprint( data )
	print_report( data )
