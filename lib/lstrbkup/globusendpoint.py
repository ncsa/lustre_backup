import datetime

class GlobusEndpoint( object ):

	reactivation_threshold = 43200

	def __init__( self, name, lifetime_secs=0 ):
		self.name = name
		self.is_active = False
		self.set_expiration( lifetime_secs )
	
	def secs_remaining( self ):
		rv = 0
		if self.is_active:
			diff = self.expiration - datetime.datetime.now()
			rv = ( diff.days * 86400 ) + diff.seconds
		return rv

	def needs_activation( self ):
		""" Return True if remaining activation time is less than min,
			if expired, or not active.
		"""
		if not self.is_active:
			return True
		if self.secs_remaining() < self.__class__.reactivation_threshold:
			return True
		return False

	def set_expiration( self, lifetime_secs ):
		self.expiration = datetime.datetime.now() + datetime.timedelta( seconds=lifetime_secs )
		if lifetime_secs > 1:
			self.is_active = True
		else:
			self.is_active = False
