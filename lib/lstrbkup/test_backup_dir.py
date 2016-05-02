import backup_dir
import pprint
import logging

logging.basicConfig(
	level=logging.DEBUG,
	format="%(levelname)s-%(filename)s[%(lineno)d]-%(funcName)s %(message)s"
	)

path = "/u/system/backup/archive/u/staff/aloftus"

def print_info( info ):
	elems = ( "filename", "status", "type", "date_time" )
	for k in elems:
		print( "{0}: {1}".format( k.title(), getattr( info, k ) ) )
	print("")

bd = backup_dir.Backup_Dir( path )
( date, info ) = bd.last_backup_attempt()
print( "LAST BACKUP" )
print_info( info )

( date, info ) = bd.last_successful_backup()
print( "LAST SUCCESSFUL BACKUP" )
print_info( info )

( date, info ) = bd.last_full_backup()
print( "LAST FULL BACKUP" )
print_info( info )
