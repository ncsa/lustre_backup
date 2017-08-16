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

class GlobusOnlineConnection( object ):
    """ Custom interface to globusonline to hide access details.
        Implements the Singleton design pattern.
    """

    def __new__( cls, *a, **k ):
        if not hasattr( cls, '_inst' ):
            cls._inst = super( GlobusOnlineConnection, cls ).__new__( cls, *a, **k )
        return cls._inst


    def __init__( self, *a, **k ):
        if not hasattr( self, "_firsttime" ):
            super( GlobusOnlineConnection, self ).__init__( *a, **k )
            self.isp = None
            self.tokens = None
            self._firsttime_init()

    def _firsttime_init( self ):
        self._firsttime = False
        self.isp = serviceprovider.ServiceProvider()
        if self.isp.auth_style == 'confidential':
            self._oauth_confidential_init()
        elif self.isp.auth_style == 'native':
            self._oauth_init()
        
    #
    # Use this with a confidential client registration, see: https://docs.globus.org/api/auth/reference/#client_credentials_grant
    # in .cfg:   auth_style = confidential
    def _oauth_confidential_init(self):
        """ Globus Confidential App Oauth 
        """
        client = globus_sdk.ConfidentialAppAuthClient(self.isp.globus_client_id, self.isp.globus_client_secret)
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
        auth_client = globus_sdk.NativeAppAuthClient( client_id=self.isp.globus_client_id )
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
            gt = transfer_client.get_task( task_id )
            ( code, reason, results ) = [gt[x] for x in ['code', 'message', 'value']]

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
        if code != 200:
            msg = "failed getting task details for task_id='{0}'".format( task_id )
            err = FatalGlobusError( msg, code, reason )
            logging.error( err )
            raise err
        logging.debug( "<<<Exit" )
        return results


    def get_subtask_details( self, task_id ):

        #self._verify()
        try:
            ( code, reason, results ) = self.api.task_successful_transfers( task_id )
        except ( APIError ) as e:
            msg = str( e )
            err = FatalGlobusError( msg, -1, "internal globus api error" )
            logging.error( err )
            raise err
        except ( GlobusError ) as e:
            msg = str( e )
            err = NonFatalGlobusError( msg, -1, "subtasks not yet available" )
            raise err
        if code != 200:
            msg = "failed getting subtask details for task_id='{0}'".format( task_id )
            err = FatalGlobusError( msg, code, reason )
            logging.error( err )
            raise err
        return results


    def _activate_endpoint( self, endpoint_name ):
        """ Activate the endpoint, if needed.
        """
        logging.debug( ">>>ENTER" )
        try:
            if self.auth_style == 'confidential':
                logging.debug('confidential auth')
                endpoint = self.transfer_client.endpoint_autoactivate( endpoint_name )
                self.logging(">>>>EXIT")
                return
            reqs_doc = self.transfer_client.endpoint_get_activation_requirements(endpoint_name)
            if reqs_doc['activated']:
                logging.debug("activated")
                logging.debug(">>>>EXIT")
                return
            if reqs_doc.supports_auto_activation:
                logging.debug( "Do autoactivation" )
                pubkey = [r['value'] for r in reqs_doc['DATA'] if r['name'] == 'public_key']
                lifetime_secs = int( self.isp.globus_endpoint_activation_lifetime )
                lifetime_hours = lifetime_secs / 3600
                proxy = globus_sdk.AuthClient.create_proxy_from_file(
                    issuer_cred_file=self.isp.x509_proxy,
                    public_key=pubkey, 
                    lifetime_hours=lifetime_hours )
                reqs_doc.set_requirement_value("delegate_proxy", "proxy_chain", proxy)
                endpoint = self.transfer_client.endpoint_autoactivate(
                    endpoint_name=endpoint_name,
                    filled_requirements=reqs,
                    if_expires_in=globus_sdk.AuthClient.GlobusEndpoint.reactivation_threshold)
                endpoint = self.transfer_client.endpoint_autoactivate( endpoint_name )
                endpoint.set_expiration( activation_results['expires_in'] )
                if endpoint['code'] == 'AutoActivationFailed':
                    logging.error( "AutoActivation of ", endpoint_name," failed: ", e )
                else:
                    logging.debug( ">>>EXIT" )
                    return
            # Not autoactivation
            logging.debug( "Not autoactivated, try web" )
            if not reqs_doc.supports_web_activation:
                logging.error( "Weird endpoint, no autoactivation, no web activation" )
                return
            logging.debug("Requirements: {}".format(endpoint))

        except Exception as ex:
            logging.error("_activate_endpoint failed: ", ex)
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
        filepath = self.isp.globus_token_file
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
        filepath = self.isp.globus_token_file
        with open( filepath, 'w' ) as f:
            json.dump( self.tokens, f )


    def _native_app_authentication( self ):
        """ Globus NativeApp Authentication flow. """
        import sys, select
        redirect_uri = 'https://auth.globus.org/v2/web/auth-code'
        scopes = ( 'openid email profile '
                   'urn:globus:auth:scope:transfer.api.globus.org:all' )
        auth_client = globus_sdk.NativeAppAuthClient( client_id=self.isp.globus_client_id )
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


    def __getattr__( self, name ):
        return getattr( self.transfer_client, name )


if __name__ == "__main__":
    CONF = 'conf/lustre_backup.cfg'
    import pprint
    import traceback
    import sys
    import mover
    import transfer
    try:
        logging.basicConfig( level=logging.DEBUG, format="%(asctime)s [%(filename)s(%(lineno)s)] %(message)s" )
        isp = serviceprovider.ServiceProvider()
        isp.loadConfig( CONF )
        go = GlobusOnlineConnection()

        # endpoints
        for ep in go.transfer_client.endpoint_search(filter_scope="my-endpoints"):
            print("[{}] {}".format(ep["id"], ep["display_name"]))

        print( "Task List" )
        rv = go.task_list()
        [pprint.pprint(i['task_id']) for i in rv]
        rv = go.endpoint_server_list('ncsa#BlueWaters')
        [pprint.pprint(i['uri']) for i in rv['DATA']]
        rv = go.endpoint_server_list('ncsabwbackup#ncsabwbackup')
        [pprint.pprint(i['uri']) for i in rv['DATA']]

        for ep in go.endpoint_search('ncsa'):
            print(ep['display_name'], ' ',)

        mvr = mover.Mover()
        t = transfer.Transfer.start_new(
        src_endpoint='d59900ef-6d04-11e5-ba46-22000b92c6ec',    #bw
    	dst_endpoint=isp.lts_endpoint,      #ncsabwbackup#bwbackup from conf
		src_filename='/u/sciteam/draila/2_MOD021KM.A2007184.1645.006.2014231113821.hdf',
		dst_filename='/u/sciteam/draila/2_MOD021KM.A2007184.1645.006.2014231113821.hdf',
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
