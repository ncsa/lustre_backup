###
# Provide a simple, interface to globus online.  Hide underlying
# access details to be able to switch methods without changing the
# rest of the codebase.
# This class is a Singleton and intented to be used by Transfer
# objects.
###

import logging
import serviceprovider
import globusendpoint
import globus_sdk
import os
import os.path
import ssl
import subprocess
import json
from lustrebackupexceptions import *

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
            self.transfer_client = None
            self._firsttime_init()


    def _firsttime_init( self ):
        self._firsttime = False
        self.isp = serviceprovider.ServiceProvider()
        self._verify()
        self._oauth_init()


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
        # TODO - REPLACE WITH transfer_client.submit_transfer
        # SEE ALSO globus_sdk.TransferData
        self._verify()
        for e in ( src_endpoint, tgt_endpoint ): 
            self._activate_endpoint( e )
        go_Txfr = globusonline.transfer.api_client.Transfer(
            submission_id,
            src_endpoint,
            tgt_endpoint,
            label=label,
            verify_checksum=verify_checksum )
        go_Txfr.add_item( src_fn, tgt_fn )
        try:
            ( code, reason, results ) = self.api.transfer( go_Txfr )
        except ( globusonline.transfer.api_client.ClientError ) as e:
            code = e.status_code
            reason = e.message
        if code != 202:
            raise NonFatalGlobusError( "Unable to start new transfer", code, reason )
        return results


    def submission_id( self ):
        # TODO - REPLACE WITH transfer_client.get_submission_id
        self._verify()
        ( code, reason, results ) = self.api.submission_id()
        if code != 200:
            msg = "failed getting new submission id"
            raise GlobusError( msg, code, reason )
        return results[ 'value' ]


    def get_transfer_details( self, task_id ):
        # TODO - REPLACE WITH transfer_client.get_task
        logging.debug( ">>>Enter: taskid='{0}'".format( task_id ) )
        self._verify()
        try:
            ( code, reason, results ) = self.api.task( task_id )
        # Differentiate between:
        # 1. non-fatal errors (ie: connection issues)
        #    TODO - don't know yet what a non-fatal error looks like
        # 2. fatal errors (everything else)
        except ( globusonline.transfer.api_client.InterfaceError ) as e:
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
        # TODO - REPLACE WITH transfer_client.task_successful_transfers
        self._verify()
        try:
            #( code, reason, results ) = self.api.subtask_list( task_id )
            ( code, reason, results ) = self.api.task_successful_transfers( task_id )
        except ( globusonline.transfer.api_client.InterfaceError ) as e:
            msg = str( e )
            err = FatalGlobusError( msg, -1, "internal globus api error" )
            logging.error( err )
            raise err
        except ( globusonline.transfer.api_client.ClientError ) as e:
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
        # TODO - REPLACE WITH transfer_client.endpoint_activate
        """ Activate the endpoint, if needed.
        """
        logging.debug( ">>>ENTER" )
        try:
            endpoint = self.endpoints[ endpoint_name ]
        except KeyError:
            endpoint = globusendpoint.GlobusEndpoint( endpoint_name )
        if not endpoint.needs_activation():
            return
        self._verify()
        ( code, reason, reqs ) = self.api.endpoint_activation_requirements( 
            endpoint_name=endpoint_name, type="delegate_proxy" )
        if reason != "OK":
            msg = "Error getting activation requirements for endpoint '{0}'".format( endpoint_name )
            raise GlobusError( msg, code, reason ) 
        
        pubkey = reqs.get_requirement_value("delegate_proxy", "public_key")
        lifetime_secs = int( self.isp.globus_endpoint_activation_lifetime )
        lifetime_hours = lifetime_secs / 3600
        proxy = globusonline.transfer.api_client.x509_proxy.create_proxy_from_file(
            issuer_cred_file=self.isp.x509_proxy,
            public_key=pubkey, 
            lifetime_hours=lifetime_hours )
        reqs.set_requirement_value("delegate_proxy", "proxy_chain", proxy)
        ( code, reason, activation_results ) = self.api.endpoint_activate( 
            endpoint_name=endpoint_name,
            filled_requirements=reqs,
            if_expires_in=globusendpoint.GlobusEndpoint.reactivation_threshold )
        if code != 200:
            raise GlobusError( "activate endpoint failed", code, reason )
        endpoint.set_expiration( activation_results['expires_in'] )
        logging.debug( ">>>EXIT" )


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


    def _save_tokens( self, token_reponse ):
        """ Set local tokens attribute AND save tokens to a file. """
        # Passing token_reponse as parameter allows this function to also be used as the
        # callback function for globus_sdk.RefreshTokenAuthorizer
        self.tokens = token_response.by_resource_provider
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

    import pprint
    logging.basicConfig( level=logging.DEBUG, format="%(asctime)s [%(filename)s(%(lineno)s)] %(message)s" )
    isp = serviceprovider.ServiceProvider()
    isp.loadConfig( "lustre_backup.cfg" )
    go = GlobusOnlineConnection()

    print( "Task List" )
    rv = go.task_list()
    pprint.pprint( rv )

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
