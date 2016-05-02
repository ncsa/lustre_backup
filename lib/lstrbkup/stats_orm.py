import peewee
import os

#TODO - see http://peewee.readthedocs.org/en/latest/peewee/example.html
#       about @app.before_request & @app.after_request
#TODO - see http://peewee.readthedocs.org/en/latest/peewee/cookbook.html#cookbook
#       about Bulk loading data

db_filename = '/u/system/backup/stats.db'
yabu_stats_db = peewee.SqliteDatabase( db_filename )

class BaseModel( peewee.Model ):
    class Meta:
        database = yabu_stats_db

class Backup( BaseModel ):
    basepath = peewee.CharField( index=True )
    date_time = peewee.DateTimeField( 
        index=True,
        formats=[ '%Y%m%d_%H%M%S' ] )
    catfile = peewee.CharField( null=True )
    cat_fn_prefix = peewee.CharField( null=True )
    mk_archive_elapsed_seconds = peewee.IntegerField( null=True )
    mk_archive_rate_bps = peewee.IntegerField( null=True )
    mk_archive_size = peewee.IntegerField( null=True )
    mk_catalog_elapsed_seconds = peewee.IntegerField( null=True )
    mk_catalog_rate_bps = peewee.CharField( null=True )
    mk_catalog_size = peewee.IntegerField( null=True )
    dar_fn_prefix = peewee.CharField( null=True )
    dar_ref_prefix = peewee.CharField( null=True )
    darfile = peewee.CharField( null=True )
    errfile = peewee.CharField( null=True )
    logfile = peewee.CharField( null=True )
    status = peewee.CharField( null=True )
    transfer_task_id = peewee.CharField( null=True )
    type = peewee.CharField()

    class Meta:
        primary_key = peewee.CompositeKey( 'basepath', 'date_time' )
#        indexes = ( 
#            ( ( "date_time" ), False ),
#            ( ( "basepath" ), False ),
#        )
    
if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser()
    parser.add_option( "-d", "--drop", action="store_true", help="Drop tables" )
    parser.add_option( "-c", "--create", action="store_true", help="Create tables" )
    parser.set_defaults(
        drop=False,
        create=False
    )
    ( opts, args ) = parser.parse_args()

    if opts.drop:
        os.unlink( db_filename )
        print( "Table '{0}' dropped.".format( "Backup" ) )


    if opts.create:
        yabu_stats_db.connect()
        Backup.create_table()
        print( "Table '{0}' created.".format( "Backup" ) )
        yabu_stats_db.close()

