import inspect
import Queue
from weakref import WeakKeyDictionary
import logging
import events


#TODO: lookup 'inspect.getmro' at init time, for all events, and store it in a dict
#TODO - EventManager and ServiceProvider should implement the
#       Singleton software pattern

class EventManager( object ):
	""" Event manager for implementation of Mediator software pattern
		 + Events must be processed in-order
		   - If an event handler generates new events, they should be handled
			 after all currents events in the queue
		 + Objects register for specific events and specify which method to
		   call for notification of each event

		   Implements the Borg/Monostate/StatelessProxy software pattern
	"""
	_shared_state = {}
	def __new__( cls, *a, **k ):
		obj = super( EventManager, cls ).__new__( cls, *a, **k )
		obj.__dict__ = cls._shared_state
		return obj

	def __init__( self, *a, **k ):
		if "firsttime" not in self.__class__._shared_state:
			super( EventManager, self ).__init__( *a, **k )
			self._firsttime_init()
			self.firsttime = False

	def _firsttime_init( self ):
		# listeners is a map of WeakKeyDict's
		self.listeners = dict()
		self.eventQueue = Queue.Queue()
		self.delayedEventQueue = Queue.Queue()

	def registerListener( self, boundMethodMap ):
		for ( event_type, boundMethod ) in boundMethodMap.iteritems():
			if event_type not in self.listeners:
				self.listeners[ event_type ] = WeakKeyDictionary()
			self.listeners[ event_type ] [ boundMethod.im_self ] = boundMethod

	def post( self, event ):
		logging.debug( "event={0}".format( event ) )

		self.eventQueue.put( event )
		# Special case, TickEvent causes queue to be processed
		if isinstance( event, events.TickEvent ):
			self._merge_queues()
			self.qsize_initial = self.eventQueue.qsize()
			logging.debug( "Starting new queue processing sequence, qsize={0}".format( self.qsize_initial ) )
			self._consumeEventQueue()

	def delayPost( self, event ):
		logging.debug( "event={0}".format( event ) )

		self.delayedEventQueue.put( event )

	def _consumeEventQueue( self ):
		while True:
			try:
				event = self.eventQueue.get_nowait()
			except Queue.Empty:
				logging.debug( "No More Events, Queue is empty (qsize was '{0}')".format( self.qsize_initial) )
				return
			logging.debug( "processing event={0}".format( event ) )

			typelist = [ "{0}.{1}".format( t.__module__, t.__name__ ) for t in inspect.getmro( type(event) ) ]
			for t in typelist:
				if t in self.listeners:
					for ( l, boundMethod ) in self.listeners[t].iteritems():
						#DEBUG
						#bn=boundMethod.__name__
						#logging.debug( "calling boundMethod={0}".format( bn ) )
						logging.debug( "calling boundMethod={0}".format( boundMethod ) )
						boundMethod( event )
		# all code paths that could possibly add more events to the queue
		# have been exhausted

	def _merge_queues( self ):
		uniq = {}
		while True:
			try:
				ev = self.delayedEventQueue.get_nowait()
			except Queue.Empty:
				return
			ev_id = str( ev )
			if ev_id in uniq:
				logging.debug( "deleting duplicate event ({0})".format( ev_id ) )
			else:
				uniq[ ev_id ] = 1
				logging.debug( "moving event={0}".format( ev ) )
				self.eventQueue.put( ev )


if __name__ == "__main__":
	raise UserWarning( "EventManager cannot be invoked directly" )
