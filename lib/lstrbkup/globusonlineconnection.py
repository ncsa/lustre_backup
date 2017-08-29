import versioncheck
###
# Provide a simple, interface to globus online.  Hide underlying
# access details to be able to switch methods without changing the
# rest of the codebase.
# This class is a Singleton and intented to be used by Transfer
# objects.
###

import logging
import serviceprovider
import globus_sdk
import os
import os.path
import ssl
import subprocess
import json
import pprint
from lustrebackupexceptions import *
import exceptions
import ConfigParser
import delegate_proxy

class Singleton(type):
    def __init__(cls, name, bases, dict):
        super(Singleton, cls).__init__(name, bases, dict)
        cls.instance = None 

    def __call__(cls,*args,**kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance

class GlobusOnlineConnection( object ):
    #__metaclass__ = Singleton
    """ Custom interface to globusonline to hide access details.
        Implements the Singleton design pattern.
    """
    
    def __init__( self, configfile=None ):
        self.isp = None
        self.tokens = None
        self.cfg = None
        self.cfgfile = configfile
        if configfile:
            self._read_conf()
        else:
            self.isp = serviceprovider.ServiceProvider()
            self.cfg = self.isp.cfg
        self._configure()
        if self.auth_style == 'confidential':
            self._oauth_confidential_init()
        elif self.auth_style == 'native':
            self._oauth_init()

    def getconf(self, name):
        try:
            return self.cfg.get("GENERAL", name)
        except:
            return None

    def _read_conf(self):
        """ Read a config file and return the ConfigParser object.
        INPUT: fn = string, filename to load from
        OUTPUT: instance of ConfigParser
        """
        cfg = ConfigParser.SafeConfigParser( )
        cfg.optionxform = str
        fh = open( self.cfgfile )
        cfg.readfp( fh )
        fh.close()
        self.cfg = cfg

    def _configure(self):
        self.globus_token_file = self.getconf("globus_token_file")
        self.auth_style = self.getconf("auth_style")
        self.globus_client_id = self.getconf("globus_client_id")
        self.globus_client_secret = self.getconf("globus_client_secret")
        self.globus_endpoint_activation_lifetime = self.getconf("globus_endpoint_activation_lifetime")
        self.x509_proxy = self.getconf("x509_proxy")


    #
    # Use this with a confidential client registration, see: https://docs.globus.org/api/auth/reference/#client_credentials_grant
    # in .cfg:   auth_style = confidential
    def _oauth_confidential_init(self):
        """ Globus Confidential App Oauth 
        """
        client = globus_sdk.ConfidentialAppAuthClient(self.globus_client_id, self.globus_client_secret)
        token_response = client.oauth2_client_credentials_tokens()
        #access_token = token_response.by_resource_server['transfer.api.globus.org']['access_token']
        transfer_token = token_response.by_resource_server['transfer.api.globus.org']['access_token']
        authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token)
        self.transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

    #
    # Use this one for "interactive-style" initial and and refresh-tokens: 
    # in .cfg:  auth_style = confidential
    def _oauth_init( self ):
        """ Prepare Globus Oauth
            Borrows extensively from:
            https://github.com/globus/native-app-examples/blob/master/example_copy_paste_refresh_token.py
        """
        self._load_tokens()
        transfer_tokens = self.tokens[ 'transfer.api.globus.org' ]
        auth_client = globus_sdk.NativeAppAuthClient( client_id=self.globus_client_id )
        authorizer = globus_sdk.RefreshTokenAuthorizer(
            transfer_tokens[ 'refresh_token' ],
            auth_client,
            access_token=transfer_tokens[ 'access_token' ],
            expires_at=transfer_tokens[ 'expires_at_seconds' ],
            on_refresh=self._save_tokens )
        self.transfer_client = globus_sdk.TransferClient( authorizer=authorizer )


    def start_new_transfer( self, 
        submission_id,
        src_endpoint, 
        src_fn, 
        tgt_endpoint, 
        tgt_fn,
        label=None,
        verify_checksum=False
        ): 

        for e in ( tgt_endpoint, src_endpoint ): 
            self._activate_endpoint( e )
        go_Txfr = globus_sdk.TransferData(self.transfer_client,
            src_endpoint,
            tgt_endpoint,
            label=label,
            verify_checksum=verify_checksum )
        go_Txfr.add_item( src_fn, tgt_fn )
        ret = self.transfer_client.submit_transfer( go_Txfr )
        return ret


    def submission_id( self ):
        #self._verify()
        sid = self.transfer_client.get_submission_id()
        return sid[ 'value' ]


    def get_transfer_details( self, task_id ):
        logging.debug( ">>>Enter: taskid='{0}'".format( task_id ) )
        #self._verify()
        try:
            gt = self.transfer_client.get_task( task_id )

        # Differentiate between:
        # 1. non-fatal errors (ie: connection issues)
        #    TODO - don't know yet what a non-fatal error looks like
        # 2. fatal errors (everything else)
        # Can differentiate the Exceptions, see https://globus-sdk-python.readthedocs.io/en/stable/exceptions/#globus_sdk.exc.GlobusError
        except ( GlobusError ) as e:
            msg = str( e )
            err = FatalGlobusError( msg, -1, "internal globus api error" )
            logging.error( err )
            raise err
        logging.debug( "<<<Exit" )
        return gt


    def get_subtask_details( self, task_id ):
        try:
            sd = self._transfer_client.task_successful_transfers( task_id )
        except ( APIError ) as e:
            msg = str( e )
            err = FatalGlobusError( msg, -1, "internal globus api error" )
            logging.error( err )
            raise err
        except ( GlobusError ) as e:
            msg = str( e )
            err = NonFatalGlobusError( msg, -1, "subtasks not yet available" )
            raise err
        return sd


    def _activate_endpoint( self, endpoint_name ):
        """ Activate the endpoint, if needed.
        """
        logging.debug( ">>>ENTER" )
        self._verify_proxy()
        try:
            if self.auth_style == 'confidential':
                logging.debug('confidential auth')
                endpoint = self.transfer_client.endpoint_autoactivate( endpoint_name )
                self.logging(">>>>EXIT")
                return
            reqs_doc = self.transfer_client.endpoint_get_activation_requirements(endpoint_name)
            if reqs_doc['activated']:
                logging.debug("activated")
                #logging.debug(">>>>EXIT")
                #return
            if reqs_doc.supports_auto_activation:
                logging.debug( "Do autoactivation" )
                lifetime_secs = int( self.globus_endpoint_activation_lifetime )
                lifetime_hours = lifetime_secs / 3600
                new_reqs = delegate_proxy.fill_delegate_proxy_activation_requirements(reqs_doc.data, self.x509_proxy, lifetime_hours)
                logging.debug("filled requirements")
                logging.debug( reqs_doc )
                logging.debug("new_reqs")
                logging.debug( new_reqs )
                #activation_results = self.transfer_client.endpoint_autoactivate(
                activation_results = self.transfer_client.endpoint_activate(
                    endpoint_name,
                    #endpoint_name=endpoint_name,
                    new_reqs,
                    if_expires_in=43200 )
                    #if_expires_in=globus_sdk.AuthClient.GlobusEndpoint.reactivation_threshold)
                #endpoint = self.transfer_client.endpoint_autoactivate( endpoint_name )
                logging.debug("endpoint_autoactivate complete")
                #endpoint.set_expiration( activation_results['expires_in'] )
                if activation_results["code"].startswith("AutoActivationFailed"):
                    #logging.error( "AutoActivation of ", endpoint_name," failed: ", e )
                    #logging.error( "AutoActivation of ", self.transfer_client.get_endpoint(endpoint_name)["display_name"]," failed: " )
                    logging.error( activation_results )
                else:
                    logging.debug( ">>>EXIT" )
                    return
            # Not autoactivation
            logging.debug( "Not autoactivated, try web" )
            if not reqs_doc.supports_web_activation:
                logging.error( "Weird endpoint, no autoactivation, no web activation" )
                return
            #logging.debug("Requirements: {}".format(endpoint))

        except Exception as ex:
            logging.error("_activate_endpoint failed: ")
            raise ex
        return
            
           
        if endpoint['code'] == 'AutoActivationFailed':
            logging.error('Endpoint({}) Not Active! Error! Source message: {}'
                .format(endpoint_name, endpoint['message']))
        elif endpoint['code'] == 'AutoActivated.CachedCredential':
            logging.debug('Endpoint({}) autoactivated using a cached credential.'
                .format(endpoint_name))
        elif endpoint['code'] == 'AutoActivated.GlobusOnlineCredential':
            logging.debug(('Endpoint({}) autoactivated using a built-in Globus '
                    'credential.').format(endpoint_name))
        elif endpoint['code'] == 'AlreadyActivated':
            debug('Endpoint({}) already active until at least'.format(endpoint_name))
        logging.debug( ">>>EXIT" )


    def _load_tokens( self ):
        """ Load a set of saved tokens. """
        filepath = self.globus_token_file
        if not os.path.exists( filepath ):
            self._native_app_authentication()
        with open( filepath, 'r' ) as f:
            self.tokens = json.load( f )
        if not self.tokens:
            msg = "Error: Failed to load GO tokens from file '{}'".format( filepath )
            raise GlobusError( msg=msg, code=-1, reason="" )


    def _save_tokens( self, tokens ):
        """ Set local tokens attribute AND save tokens to a file. """
        # Passing token_reponse as parameter allows this function to also be used as the
        # callback function for globus_sdk.RefreshTokenAuthorizer
        self.tokens = tokens.by_resource_server
        filepath = self.globus_token_file
        with open( filepath, 'w' ) as f:
            json.dump( self.tokens, f )


    def _native_app_authentication( self ):
        """ Globus NativeApp Authentication flow. """
        import sys, select
        redirect_uri = 'https://auth.globus.org/v2/web/auth-code'
        scopes = ( 'openid email profile '
                   'urn:globus:auth:scope:transfer.api.globus.org:all' )
        auth_client = globus_sdk.NativeAppAuthClient( client_id=self.globus_client_id )
        auth_client.oauth2_start_flow( requested_scopes=scopes,
                                       redirect_uri=redirect_uri,
                                       refresh_tokens=True )
        auth_url = auth_client.oauth2_get_authorize_url()
        timeout = 300
        print( 'Auth URL:\n{}'.format( auth_url ) )
        print( 'Enter auth code: (timeout in {} seconds) \n'.format( timeout ) )
        i, o, e = select.select( [sys.stdin], [], [], timeout )
        if i:
            auth_code = sys.stdin.readline().strip()
        else:
            raise GlobusError( 
                msg='No auth code found on std input',
                code=-1,
                reason='Timed out waiting for response' )
        token_response = auth_client.oauth2_exchange_code_for_tokens( auth_code )
        self._save_tokens( token_response )

    def _verify( self ):
        self._verify_proxy()


    def _verify_proxy( self ):
        logging.debug( ">>>ENTER" )
        proxyfile = self.isp.x509_proxy
        needs_reset = True
        if os.path.isfile( proxyfile ):
            cmd = [ "/usr/bin/grid-proxy-info",
                "-exists",
                "-file", proxyfile,
                "-valid", "12:00" ]
            rc = subprocess.call( cmd )
            if rc == 0:
                needs_reset = False
        if needs_reset:
            self._reset_proxy()
        logging.debug( ">>>EXIT" )


    def _reset_proxy( self ):
        logging.debug( ">>>ENTER" )
        secs_raw = int( self.isp.globus_proxy_lifetime )
        valid = "{0}:{1}".format( secs_raw / 3600, ( secs_raw % 3600 ) / 60 )
        cmd = [ "/usr/bin/grid-proxy-init",
            "-cert", self.isp.x509_cert,
            "-key", self.isp.x509_key,
            "-out", self.isp.x509_proxy,
            "-valid", valid ]
        subp = subprocess.Popen( cmd, 
            stdout=subprocess.PIPE, stderr=subprocess.PIPE )
        ( output, errput ) = subp.communicate()
        rc = subp.returncode
        if rc != 0:
            raise GlobusError( msg="Failed to reset proxy.",
                code=rc,
                reason=errput )
        logging.debug( ">>>EXIT" )

if __name__ == "__main__":
    CONF = 'conf/lustre_backup.cfg'
    import pprint
    import traceback
    import sys
    import mover
    import transfer
    import events
    try:
        logging.basicConfig( level=logging.DEBUG, format="%(asctime)s [%(filename)s(%(lineno)s)] %(message)s" )
        
        go = GlobusOnlineConnection(CONF)
        #go.configure(CONF)
        #go._firsttime_init()

        isp = serviceprovider.ServiceProvider()
        isp.loadConfig( CONF )

        ''' go = GlobusOnlineConnection()
        # endpoints
        for ep in go.transfer_client.endpoint_search(filter_scope="my-endpoints"):
            print("[{}] {}".format(ep["id"], ep["display_name"]))

        print( "Task List" )
        rv = go.transfer_client.task_list()
        [pprint.pprint(i['task_id']) for i in rv]
        rv = go.transfer_client.endpoint_server_list('ncsa#BlueWaters')
        [pprint.pprint(i['uri']) for i in rv['DATA']]
        rv = go.transfer_client.endpoint_server_list('ncsabwbackup#ncsabwbackup')
        [pprint.pprint(i['uri']) for i in rv['DATA']]

        for ep in go.transfer_client.endpoint_search('ncsa'):
            print(ep['display_name'], ' ',) '''
 
        mvr = mover.Mover()
        t = transfer.Transfer.start_new(
        src_endpoint='d59900ef-6d04-11e5-ba46-22000b92c6ec',    #bw
    	dst_endpoint=isp.lts_endpoint,      #ncsabwbackup#bwbackup from conf
		src_filename='/u/sciteam/draila/aa',
		dst_filename='/projects/BW-System/bw_lustre_backups/aa',
        basepath='/' )
        #	print( "Initiated transfer: {0}".format( t ) )
        mvr._save_transfer( t )
        print( "Saved transfer: {0}".format( t ) )
        print( "Transfer status:\n{0}".format( pprint.pformat( t.info ) ) )
        mvr.check_open_transfers( events.StartOfSemidayEvent() )

        print ('Bye')
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


#    go._verify_proxy()
#
#    print( "{0}\n>>>GLOBUS TASKSUMMARY\n{0}".format( "--------------------" ) )
#    try:
#        results = go.api.tasksummary()
#    except (ssl.SSLError) as e:
#        print( "Caught SSL error: errno='{0}', strerror='{1}'".format( e.errno, e.strerror ) )
#    else:
#        pprint.pprint( results )
#
#    print( "{0}\n>>>ACTIVATE ENDPOINT ncsabwbackup#Nearline\n{0}".format( "--------------------" ) )
#    go._activate_endpoint( "ncsabwbackup#Nearline" )
#    print( "Activation Successful\n" ) #exception will be thrown if activation failure
#    print( "{0}\n>>>ACTIVATE ENDPOINT ncsa#BlueWaters\n{0}".format( "--------------------" ) )
#    status = go._activate_endpoint( "ncsa#BlueWaters" )
#    print( "Activation Successful\n" ) #exception will be thrown if activation failure
#
#    test_taskid = '861d66d2-1655-11e3-9f31-22000a972bd6'
#    print( "{0}\n>>>GET TRANSFER DETAILS\n{0}".format( "--------------------" ) )
#    status = go.get_transfer_details( test_taskid )
#    pprint.pprint( status )
#
#    print( "{0}\n>>>GET SUBTASK DETAILS\n{0}".format( "--------------------" ) )
#    details = go.get_subtask_details( test_taskid )
#    pprint.pprint( details )
