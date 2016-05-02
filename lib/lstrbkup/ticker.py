import events
import time
import datetime
import eventmanager
import serviceprovider
import logging

class Ticker( object ):
	def __init__( self ):
		self.ev_mgr = eventmanager.EventManager()
		self.isp = serviceprovider.ServiceProvider()
		self.keep_going = True
		self.now = datetime.datetime.now()
		self.prev_start_of_day = datetime.datetime( datetime.MINYEAR, 1, 1 )
		self.prev_semiday = datetime.datetime( datetime.MINYEAR, 1, 1 )
		self.prev_semihour = datetime.datetime( datetime.MINYEAR, 1, 1 )
		self.tick_interval = int( self.isp.tick_interval )
		self.day_delta = self._convert_secs_to_delta( int( self.isp.day_interval ) )
		self.semiday_delta = self._convert_secs_to_delta( int( self.isp.semiday_interval ) )
		self.semihour_delta = self._convert_secs_to_delta( int( self.isp.semihour_interval ) )
		self.ev_mgr.registerListener( { 
			'events.QuitEvent': self.quit, 
			'events.StartOfDayEvent': self.update_start_of_day, 
			'events.StartOfSemidayEvent': self.update_prev_semiday,
			'events.StartOfSemihourEvent': self.update_prev_semihour,
			} )

	def run( self ):
		while self.keep_going:
			self.now = datetime.datetime.now()
			logging.debug( "NEW TICK ({0})".format( self.now ) )
			if self.is_start_of_day():
				self.ev_mgr.post( events.StartOfDayEvent() )
				self.ev_mgr.post( events.StartOfSemidayEvent() )
				self.ev_mgr.post( events.StartOfSemihourEvent() )
			elif self.is_start_of_semiday():
				self.ev_mgr.post( events.StartOfSemidayEvent() )
				self.ev_mgr.post( events.StartOfSemihourEvent() )
			elif self.is_start_of_semihour():
				self.ev_mgr.post( events.StartOfSemihourEvent() )
			self.ev_mgr.post( events.TickEvent() )
			time.sleep( self.tick_interval )

	def is_start_of_day( self ):
		rv = False
		diff = self.now - self.prev_start_of_day
		if diff > self.day_delta:
			rv = True
		return rv

	def is_start_of_semiday( self ):
		rv = False
		diff = self.now - self.prev_semiday
		if diff > self.semiday_delta:
			rv = True
		return rv

	def is_start_of_semihour( self ):
		rv = False
		diff = self.now - self.prev_semihour
		if diff > self.semihour_delta:
			rv = True
		return rv

	def update_start_of_day( self, ev ):
		self.prev_start_of_day = self.now

	def update_prev_semiday( self, ev ):
		self.prev_semiday = self.now

	def update_prev_semihour( self, ev ):
		self.prev_semihour = self.now

	def quit( self, event ):
		self.keep_going = False

	def _convert_secs_to_delta( self, raw_secs ):
		days = raw_secs / 86400
		secs = raw_secs % 86400
		return datetime.timedelta( days=days, seconds=secs )

if __name__ == "__main__":
	import os
	isp = serviceprovider.ServiceProvider()
	isp.loadConfig( os.path.join( os.getcwd(), "lustre.backup.cfg.ticker.test" ) )
	ticker = Ticker()
	ticker.run()
