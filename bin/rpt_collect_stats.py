#!python
import versioncheck
import os
import datetime
import pprint
import optparse
import logging
logging.basicConfig(
    format="%(asctime)s.%(msecs)d - %(levelname)s - %(funcName)s[%(lineno)d] - %(message)s",
    datefmt="%H%M%S",
    level=logging.DEBUG )

#local imports
import peewee
import lstrbkup.backup_dir
import lstrbkup.mover
import lstrbkup.stats_orm

OPTS = None
ARCHDIR = "/u/system/backup/archive"


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


def get_all_backup_dirs( basepath ):
    """ Find all leaf dirs below basepath
    """
    backup_dirs = []
    for ( base, dirs, files ) in os.walk( basepath ):
        if len( dirs ) < 1:
            backup_dirs.append( lstrbkup.backup_dir.Backup_Dir( base ) )
    return backup_dirs


def get_db_max_date( db ):
    max_date = datetime.datetime.min
    try:
        max_date = lstrbkup.stats_orm.Backup.select().aggregate( 
            peewee.fn.Max( lstrbkup.stats_orm.Backup.date_time ) )
    except ( peewee.OperationalError ) as e:
        if 'no such table: backup' in e.args:
             pass
        else:
            raise e
    # If table is empty, max_date will be None
    if max_date == None:
        max_date = datetime.datetime.min
    return max_date


def test_me():
    logging.debug( "Scanning backup dirs" )
    all_dirs = get_all_backup_dirs( OPTS.dir )
    db = lstrbkup.stats_orm.yabu_stats_db
    db.connect()
#    logging.debug( "Querying for last max date" )
#    max_date = get_db_max_date( db )
#    logging.debug( "Last max date: '{0}'".format( max_date ) )
    for bkup_dir in all_dirs:
        basepath = mk_basepath( bkup_dir.path )
        logging.info( "Processing directory: '{0}'".format( bkup_dir.path ) )
        with db.transaction():
            for ( bkup_date, bkup_info ) in bkup_dir.iter_backups():
#                if bkup_date <= max_date:
#                    continue
                bkup_data = bkup_info.as_dict()
                add_basepath( basepath, bkup_data )
                clean_data = normalize_backup_data( bkup_data )
                try:
                    record = lstrbkup.stats_orm.Backup.create( **clean_data )
                except ( peewee.IntegrityError ) as e:
                    logging.warning( e )


def mk_basepath( dn ):
    #TODO: get ARCHDIR from config file via serviceprovider
    return dn.replace( ARCHDIR, "" )


def add_basepath( bp, data ):
    if bp not in data:
        data[ "basepath" ] = bp

def normalize_backup_data( d ):
    clean_data = {}
    old_keys_map = {
        "dar_archive_elapsed_seconds": "mk_archive_elapsed_seconds",
        "dar_archive_rate_bps":        "mk_archive_rate_bps",
        "dar_archive_size":            "mk_archive_size",
        "dar_catalog_elapsed_seconds": "mk_catalog_elapsed_seconds",
        "dar_catalog_rate_bps":        "mk_catalog_rate_bps",
        "dar_catalog_size":            "mk_catalog_size",
    }
    for (k,v) in d.iteritems():
        if k in old_keys_map:
            k = old_keys_map[ k ]
        if "_rate_" in k:
            clean_data[ k ] = v.split( '.', 1 )[0]
            print( "Normalize {0}:{1}".format( k, clean_data[ k ] ) )
        else:
            clean_data[ k ] = v
    return clean_data


if __name__ == "__main__":
    ( OPTS, args ) = process_options()
    db_logger = logging.getLogger( 'peewee' )
    db_logger.setLevel( logging.WARNING )
    db_logger.addHandler( logging.StreamHandler() )
    if OPTS.debug:
        db_logger.setLevel( logging.DEBUG )
    starttime = datetime.datetime.now()
    test_me()
    endtime = datetime.datetime.now()
    diff = endtime - starttime
    logging.info( "Total time: {0}".format( diff ) )
