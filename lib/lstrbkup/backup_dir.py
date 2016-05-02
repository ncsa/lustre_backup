#!/usr/bin/python

import os
import os.path
import datetime
import ConfigParser

#local imports
import dar
import infofile
import mover
import serviceprovider
from lustrebackupexceptions import *

import logging
logr = logging.getLogger( __name__ )

# TODO - User dar.py as a library for interacting with each individual
#        backup

class Backup_Dir( object ):
    # TODO - should these move to the main config file?
    LOCKFILENAME = ".lustrebackup.lck"
    INFOFILENAME = "backup.info"

    def __init__( self, path ):
        """ path is the absolute path to the archive dir (not the user
            home dir)
        """
        self.isp = serviceprovider.ServiceProvider()
        self.path = path
        self.backups = {}
        self.locked = False
        self._load()
        #backup dates in reverse sorted order (newest first)
        self.iter_keys = sorted( self.backups.keys(), reverse=True )

    def _load( self ):
        all_files = os.listdir( self.path )
        for fn in all_files:
            if fn.endswith( self.INFOFILENAME ):
                bkupinfo_fn = os.path.join( self.path, fn )
                bkupinfo = Backup_Info_File( bkupinfo_fn )
                try:
                    valid = bkupinfo.is_valid()
                except ( ZeroLengthFileError ) as e:
                    logr.warn( e )
                    continue
                if valid:
                    bkup_date = bkupinfo.get_datetime( 'date_time' )
                else:
                    bkup_date = bkupinfo.getmtime()
                    bkupinfo.set( "status", "INVALID" )
                self.backups[ bkup_date ] = bkupinfo
            elif fn == self.LOCKFILENAME:
                self.locked = True

    def __str__( self ):
        return "<Backup_Dir ({0})>".format( self.path )
    __repr__ = __str__


    def last_successful_backup( self ):
        """ Returns a tuple (bkup_date, bkup_info)
            where bkup_info is an instance of InfoFile
            representing the contents of the backup.info file for an
            individual backup
        """
        ### TODO - This is the correct way to do it, but
        ###        dar.BACKUP_COMPLETE is new so have to wait for it
        ###        to propogate out
        #filter = { "status", dar.BACKUP_COMPLETE }
        #return self._last_backup_by_filter( filter=filter )
        bkup_date = None
        bkup_info = None
        for date in self.iter_keys:
            logr.debug( "Checking date: {0}".format( date ) )
            info = self.backups[ date ]
            fn_base = os.path.basename( info.filename )
            #logr.debug( "fn status:{1} fn.len:{0} fn:{2}".format( len( fn_base ), info.status, fn_base ) )
            if len( fn_base ) == 27 and info.status in ( dar.BACKUP_COMPLETE, mover.TRANSFER_FINISHED ):
                bkup_date = date
                bkup_info = info
                logr.debug( "MATCH ({0}) (lastBU={1}) (type={2})".format(
                    self.path, bkup_date, bkup_info.type ) )
                break
        return ( bkup_date, bkup_info )


    def last_full_backup( self ):
        """ Last successful, full, backup.
            Returns a tuple (bkup_date, bkup_info)
        """
        ### TODO - This is the correct way to do it, but
        ###        dar.BACKUP_COMPLETE is new so have to wait for it
        ###        to propogate out
#        filter = { 
#            "status", dar.BACKUP_COMPLETE,
#            "type", dar.FULL,
#            }
#        return self._last_backup_by_filter( filter=filter )
        bkup_date = None
        bkup_info = None
        for date in self.iter_keys:
            info = self.backups[ date ]
            fn_base = os.path.basename( info.filename )
            logr.debug( "fn status:{0} type:{1} fn.len:{2} fn:{3}".format( info.status, info.type, len( fn_base ), fn_base ) )
            if len( fn_base ) == 27 and info.status in ( dar.BACKUP_COMPLETE, mover.TRANSFER_FINISHED ) and info.type == dar.FULL:
                bkup_date = date
                bkup_info = info
                break
        return ( bkup_date, bkup_info )


    def last_backup_attempt( self ):
        """ Similar to last_successful_backup, but without status
            check.
        """
        return self._last_backup_by_filter()


    def _last_backup_by_filter( self, filter={} ):
        """ Search all backups in reverse date order and return the
            first one that matches all the filter paramters.
        """
        bkup_date = None
        bkup_info = None
        for date in self.iter_keys:
            info = self.backups[ date ]
            match = True
            for (k,v) in filter.iteritems():
                val = getattr( info, k )
                if val != v:
                    match = False
                    break
            if match:
                bkup_date = date
                bkup_info = info
                break
        return ( bkup_date, bkup_info )
        

    def iter_backups( self, oldest_first=False ):
        """ Iterate through all backups in date sorted order (newest
            first by default).  
            Return tuple of ( datetime, infofile ).
        """
        keys = self.iter_keys
        if oldest_first:
            keys = reversed( self.iter_keys )
        for k in keys:
            yield ( k, self.backups[ k ] )


    def is_locked( self ):
        return self.locked


    def is_backup_currently_running( self ):
        """ Determine if a backup is actively running.
        """
#  This is delicate because:
#  1. Want to start working on INCR's as soon as all FULL's
#     are started.
#  2. For backups in transfer, dir is not locked.  If the
#     manager restarts (existing events will be lost) AND
#     globusonline_open_transfers list gets wiped out, then
#     active transfers will never get checked (since the only
#     way they will get checked is to be included in the
#     NEEDS_BACKUP list during filesystemscan for dirs needed
#     a backup.
#  Solution 1: return active if in transfer state but less
#     than X secs old (where X = 1 day?).  
#     How to check age?
#     Using last_info.date_time will constantly report active
#     backup after X time from start time, not last check
#     (very long running transfer can block all new backups).
#  Q1: Add last_status_check timestamp to infofile?
#  A1: Yes.  Here just check last_status_check (use infofile last
#      modified date otherwise).  Any other part of the backup
#      process (ie: mover.check_transfer_status, etc...) should
#      update last_status_check as appropriate.  Thus, a long 
#      running transfer will get checked periodically and
#      last_status_check can be updated each time.
        if self.is_locked():
            return True
        ( last_date, last_info ) = self._last_backup_by_filter()
        if last_info is not None:
            if last_info.status != dar.BACKUP_COMPLETE:
                try:
                    last_date = last_info.get_datetime( 'last_status_check' )
                except ( FileError ) as e:
                    last_date = last_info.getmtime()
                #TODO - hours=24 should come from a config variable
                max_age = datetime.datetime.now() - datetime.timedelta( hours=24 )
                if last_date > max_age:
                    return True
        return False


    def unlock( self, reason ):
        self.isp.releaseDirLockByForce( self.path, reason )
        self.locked = False


    def reset( self, reason ):
        ( bkup_date, bkup_info ) = self.last_backup_attempt()
        if bkup_info is not None:
            pfx = bkup_info.date_time
            bkup_info.rename_auto_date_sfx( reason=reason, pfx=pfx )
        self.unlock( reason )


    def del_backups_older_than( self, mindays ):
        minage = datetime.datetime.now() - datetime.timedelta( mindays )
        for ( bkupdate, bkupinfo ) in self.iter_backups( oldest_first=True ):
            logr.debug( 'Found bkup: {0}'.format( bkupdate ) )
            print( 'Found bkup: {0}'.format( bkupdate ) )
            if bkupdate < minage:
                #delete all files associated with this backup
                print( '  OLD - DELETE ME' )
                for f in [ 'errfile', 'logfile', 'darfile', 'catfile', 'filename' ]:
                    fn = getattr( bkupinfo, f )
                    try:
                        os.unlink( fn )
                    except ( OSError ) as e:
                        if e.errno == 2:
                            pass
                        else:
                            raise e


class Backup_Info_File( infofile.InfoFile ):
    required_fields = (
        "type",
        "date_time",
        "errfile",
        "logfile",
        "darfile",
        "catfile",
        "dar_fn_prefix",
        "cat_fn_prefix",
        )

    def is_valid( self ):
        # check for zero length file
        if os.path.getsize( self.filename ) < 1:
            raise ZeroLengthFileError( self.filename )
        try:
            d = self.as_dict()
        except ( ConfigParser.NoSectionError ) as e:
            return False
        for k in Backup_Info_File.required_fields:
            if k not in d:
                return False
        return True
