#!/opt/rh/python27/root/usr/bin/python2.7
#import versioncheck
import os
import os.path
import datetime
import pprint
import sys
import optparse
import logging
logging.basicConfig(
    format="%(asctime)s.%(msecs)d - %(levelname)s - %(funcName)s[%(lineno)d] - %(message)s",
    datefmt="%H%M%S",
    level=logging.INFO )

logr = logging.getLogger( __name__ )

#local imports
import lstrbkup.backup_dir
import lstrbkup.mover
from lstrbkup.lustrebackupexceptions import *

TODAY = datetime.datetime.now()
NONE_AGE = datetime.timedelta( seconds=0 )
OPTS = None
DIR_SUMMARY_FMT = "{date:19s}  {age:12s}  {type:4s}  -e {status:15s}  -d {path}"
HDR_LEN = 72

###
# xtra: interactive ( ask clean, skip, quit for each dir )
# xtra: delete incomplete dar files (archive and catalog)
# xtra: determine completeness of dar file (archive and catalog)
# xtra: delete old data (or archive for data mining purposes)
###


def process_options():
    parser = optparse.OptionParser()
    parser.add_option( "-f", "--fix", action="store_true",
        help="fix locked dirs [default: %default]" )
    default_err_types = [
        lstrbkup.mover.TRANSFER_FAILED, 
        lstrbkup.dar.CREATE_FAILED,
        lstrbkup.dar.CATALOG_FAILED,
        ]
    other_err_types = [
        lstrbkup.mover.TRANSFER_STARTED, 
        lstrbkup.mover.TRANSFER_FINISHED, 
        lstrbkup.dar.CREATE_STARTED,
        lstrbkup.dar.CREATE_FINISHED,
        lstrbkup.dar.CATALOG_STARTED,
        lstrbkup.dar.CATALOG_FINISHED,
        lstrbkup.dar.DAR_COMPLETE,
        "dar_started",
        "dar_failed",
        "INVALID",
        ]
    all_status_types = default_err_types + other_err_types
    parser.add_option( "-d", "--dir", help="Dir to start scan.  [Default: %default]" )
    parser.add_option( "-m", "--minage", type="int",
        help="Num secs, only fix locked dirs that are older than this num secs. [default: %default]" )
    parser.add_option( "-e", "--errtypes", action="append", 
        choices=all_status_types, metavar="ERRTYPE",
        help="Only fix the specified error type(s).  This option can be repeated multiple times.  Available choices: {0} [default: %default]".format( sorted( all_status_types ) ) )
#    parser.add_option( "--reset", action="store_true", help="Force dir reset.  [Default: %default]" )
    parser.add_option( "--clean", action="store_true", help="Force clean dir (delete any files associated with last backup attempt).  Only applies to specified 'errtypes'.[Default: %default]" )
    parser.add_option( "--rmdays", metavar='N',
        help="Remove backup records older than N days." )
    parser.add_option( '-D', '--debug', action="store_true" )
    parser.add_option( '-M', '--missing_dars', action="store_true",
        help="Show missing dar report (experimenta)" )
    parser.set_defaults( 
        dir="/u/system/backup/archive",
        fix=False,
        errtypes=None,
        minage=259200,
#        reset=False,
        clean=False,
        )
    ( options, args ) = parser.parse_args()
    if options.errtypes is None:
        options.errtypes = default_err_types
    if options.rmdays:
        options.rmdays = int( options.rmdays )
    return ( options, args )


def get_all_backup_dirs( basepath ):
    """ Find all leaf dirs below basepath
    """
    backup_dirs = []
    for ( base, dirs, files ) in os.walk( basepath ):
        if len( dirs ) < 1:
            backup_dirs.append( lstrbkup.backup_dir.Backup_Dir( base ) )
    return backup_dirs


def pretty_timedelta( td ):
    day_str = ""
    secs = td.seconds
    h = td.seconds / 3600
    secs = td.seconds % 3600
    m = secs / 60
    s = secs % 60
    return "{d:03d}:{h:02d}:{m:02d}:{s:02d}".format( d=td.days, h=h, m=m, s=s )


def dir_summary_data( d ):
    age = NONE_AGE
    data = { "date": None, "raw_age": None, "age": age, "type": None, "status": None, "path": d.path }
    ( bkup_date, bkup_info ) = d.last_backup_attempt()
    if bkup_date is not None:
            data[ "date" ] = str( bkup_date )
    if bkup_info is not None:
        data[ "status" ] = bkup_info.status
        if bkup_info.is_valid():
            age = TODAY - bkup_date
            data[ "raw_age" ] = age
            data[ "age" ] = pretty_timedelta( age )
            data[ "type" ] = bkup_info.type
#        else:
#            data[ "status" ] = "INVALID"
    return data


def print_dir_current_stats( dirs ):
    print( DIR_SUMMARY_FMT.format(     
        date="Last_Bkup_Attempt", 
        age="Age",
        type="Type",
        status="Status", 
        path="Basepath" ) )
    data_by_age = {}
    for d in dirs:
        data = dir_summary_data( d )
        age = data[ "raw_age" ]
        if age is None:
            print( DIR_SUMMARY_FMT.format( **data ) )
            continue
        if age not in data_by_age:
            data_by_age[ age ] = []
        data_by_age[ age ].append( data )
    for k in sorted( data_by_age.keys() ):
        for d in data_by_age[ k ]:
            print( DIR_SUMMARY_FMT.format( **d ) )
        

def fix_locked( dirs ):
    unfixed_dirs = []
    fixed_dirs = []
    for d in dirs:
        ( bkup_date, bkup_info ) = d.last_backup_attempt()
        #skip dirs without date
        if bkup_date is None:
            logr.debug( "Skipping dir {0}, has no date".format( d ) )
            unfixed_dirs.append( d )
            continue
        bkup_status = bkup_info.status
        #skip unspecified errtypes
        if bkup_status not in OPTS.errtypes:
            unfixed_dirs.append( d )
            continue
        if OPTS.clean:
            func_name = "_clean_and_reset_backup_dir"
        else:
            func_name = "_fix_{0}".format( bkup_status )
        func = getattr( sys.modules[ __name__ ], func_name, _not_implemented )
        result = func( d, bkup_status )
        if result is True:
            fixed_dirs.append( d )
        else:
            unfixed_dirs.append( d )
    return ( fixed_dirs, unfixed_dirs )

def _not_implemented( *a, **k ):
    logr.warn( "a:{0} k:{1}".format( pprint.pformat( a ), pprint.pformat( k ) ) )
    return False

def _reset_backup_dir( dir, reason ):
    logr.debug( "attempting to fix {0} ({1})".format( dir, reason ) )
    dir.reset( reason )
    return True

def _clean_backup_dir( dir, reason ):
    ( bkup_date, bkup_info ) = dir.last_backup_attempt()
    for i in [ 'darfile','catfile' ]:
        try:
            f = getattr( bkup_info, i )
        except FileError:
            continue
        logr.debug( "about to delete '{0}'".format( f ) )
        try:
            os.unlink( f )
        except ( OSError ) as e:
            logr.debug( "caught err: {0}".format( e ) )
    return True

def _clean_and_reset_backup_dir( dir, status ):
    logr.debug( "dir:{0} status:{1}".format( dir, status ) )
    reason = "Forced cleanup of stuck '{0}' process".format( status )
    _clean_backup_dir( dir, reason )
    _reset_backup_dir( dir, reason )
    return True

_fix_catalog_started = _clean_and_reset_backup_dir
_fix_catalog_failed  = _clean_and_reset_backup_dir
_fix_create_started  = _clean_and_reset_backup_dir
_fix_create_failed   = _clean_and_reset_backup_dir
_fix_dar_failed      = _clean_and_reset_backup_dir
_fix_INVALID         = _clean_and_reset_backup_dir
    
def _fix_txfr_failed( dir, status ):
    logr.debug( "attempting to fix {0}".format( dir ) )
    #Set status to lstrbkup.dar.BACKUP_SUCCESS so transfer can be tried again
    # 1. update status
    ( bkup_date, bkup_info ) = dir.last_backup_attempt()
    bkup_info.set( "status", lstrbkup.dar.DAR_COMPLETE )
    # 2. unlock dir
    dir.unlock( reason=status )
    return True


def _fix_txfr_finished( dir, status ):
    """ Transfer is finished, so just unlock dir and let cleanup
        happen normally.
    """
    logr.debug( "attempting to fix {0}".format( dir ) )
    dir.unlock( reason="Forced unlock for status='{0}'.".format( status ) )
    return True

_fix_create_finished = _fix_txfr_finished
_fix_create_complete = _fix_txfr_finished
_fix_dar_complete = _fix_txfr_finished


def del_old_backups():
    for bkupdir in get_all_backup_dirs( OPTS.dir ):
        logr.debug( 'Processing bkupdir: {0}'.format( bkupdir ) )
        bkupdir.del_backups_older_than( OPTS.rmdays )

def fix_or_report():
    bkup_dirs = get_all_backup_dirs( OPTS.dir )
    locked_dirs = [] #locked
    missing_dar_dirs = [] # darfile DNE
    min_bkup_date = TODAY - datetime.timedelta( seconds=OPTS.minage )
    for dir in bkup_dirs:
        ( bkup_date, bkup_info ) = dir.last_backup_attempt()
        #skip dirs without date (if date is empty, so is info)
        if bkup_date is None:
            logr.debug( "Skipping dir {0}, has no date".format( dir ) )
            continue
        if dir.is_locked():
            if bkup_date < min_bkup_date:
                locked_dirs.append( dir )
            else:
                logr.debug( "Skipping dir {0}, lock is younger than {1} secs".format( dir, OPTS.minage ) )
        elif not os.path.isfile( bkup_info.darfile ):
            missing_dar_dirs.append( dir )

    if len( locked_dirs ) > 0:
        print( "Locked Dirs".center( HDR_LEN, '-' ) )
        print_dir_current_stats( locked_dirs )

    if OPTS.missing_dars and len( missing_dar_dirs ) > 0:
        print( "Missing Darfile".center( HDR_LEN, '-' ) )
        print_dir_current_stats( missing_dar_dirs )

#    #TEST iter_backups
#    d = locked_dirs[0]
#    print( "Backup Dates default sorted order" )
#    for ( bkup_date, bkup_info ) in d.iter_backups():
#        print( bkup_date )
#    print( "Backup Dates reverse sorted order (oldest first)" )
#    for ( bkup_date, bkup_info ) in d.iter_backups( oldest_first=True ):
#        print( bkup_date )

    if OPTS.fix:
        fixd, unfixd = fix_locked( locked_dirs )
        print( "Fixed Dirs".center( HDR_LEN, '-' ) )
        print_dir_current_stats( fixd )
        print( "Unfixed Dirs".center( HDR_LEN, '-' ) )
        print_dir_current_stats( unfixd )


if __name__ == "__main__":
    ( OPTS, args ) = process_options()
    if OPTS.debug:
        logr.setLevel( logging.DEBUG )
    if OPTS.rmdays:
        logr.debug( 'got rmdays={0}'.format( OPTS.rmdays ) )
        del_old_backups()
    else:
        fix_or_report()
    endtime = datetime.datetime.now()
    diff = endtime - TODAY
    logr.info( "Total time: {0}".format( diff ) )
