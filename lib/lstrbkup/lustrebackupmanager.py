#!
import versioncheck

#Native/installed imports
import os
import os.path
import sys

#Custom imports
import eventmanager
import serviceprovider
import events
import jobmanager
import logging
import logging.config
import mover
import ticker
import scheduler

#logging.config.fileConfig( os.path.join( os.path.dirname( __file__), 'logging.manager.cfg' ) )
logging.config.fileConfig( os.path.join( 
	os.getenv( "LSTRBKUPCONFDIR" ), 
	'logging.manager.cfg' ) )

def run():
	evMgr = eventmanager.EventManager()
	isp = serviceprovider.ServiceProvider()
	cfg_fn = os.path.join( 
		os.getenv( "LSTRBKUPCONFDIR" ), 
		"lustre_backup.cfg" )
	isp.loadConfig( cfg_fn )
	jobMgr = jobmanager.JobManager()
	sched = scheduler.Scheduler()
	movr = mover.Mover()
	timer = ticker.Ticker()

	timer.run()

if __name__ == "__main__":
	#raise SystemExit( "Cannot invoke lustre_backup_manager directly." )
	run()
