import os
import requests
import time
import traceback

from twisted.internet import threads, reactor, defer

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusController
from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusNumbers
from hydrus.core import HydrusPaths
from hydrus.core import HydrusSessions
from hydrus.core import HydrusTime
from hydrus.core.networking import HydrusNetwork
from hydrus.core.networking import HydrusNetworking
from hydrus.core.processes import HydrusProcess
from hydrus.core.processes import HydrusThreading

from hydrus.server import ServerDB
from hydrus.server import ServerFiles
from hydrus.server import ServerGlobals as SG
from hydrus.server.networking import ServerServer

def process_starting_action( db_dir, action ):
    
    already_running = HydrusProcess.is_already_running( db_dir, 'server' )
    
    if action == 'start':
        
        if already_running:
            
            HydrusData.print_text( 'The server is already running. Would you like to [s]top it, [r]estart it here, or e[x]it?' )
            
            answer = input()
            
            if len( answer ) > 0:
                
                answer = answer[0]
                
                if answer == 's':
                    
                    return 'stop'
                    
                elif answer == 'r':
                    
                    return 'restart'
                    
                
            
            return 'exit'
            
        else:
            
            return action
            
        
    elif action == 'stop':
        
        if already_running:
            
            return action
            
        else:
            
            raise HydrusExceptions.ShutdownException( 'The server is not running, so it cannot be stopped!' )
            
        
    elif action == 'restart':
        
        if already_running:
            
            return action
            
        else:
            
            HydrusData.print_text( 'Did not find an already running instance of the server--changing boot command from \'restart\' to \'start\'.' )
            
            return 'start'
            
        
    

def shutdown_sibling_instance( db_dir ):
    
    port_found = False
    
    try:
        
        ports = HydrusProcess.get_sibling_process_ports( db_dir, 'server' )
        
    except HydrusExceptions.CancelledException as e:
        
        HydrusData.print_text( e )
        
        ports = None
        
    
    if ports is None:
        
        raise HydrusExceptions.ShutdownException( 'Could not figure out the existing server\'s ports, so could not shut it down!' )
        
    
    session = requests.Session()
    
    session.verify = False
    
    for port in ports:
        
        try:
            
            r = session.get( 'https://127.0.0.1:' + str( port ) + '/' )
            
            server_name = r.headers[ 'Server' ]
            
        except Exception as e:
            
            text = 'Could not contact existing server\'s port ' + str( port ) + '!'
            text += '\n'
            text += traceback.format_exc()
            
            raise HydrusExceptions.ShutdownException( text )
            
        
        if 'server administration' in server_name:
            
            port_found = True
            
            HydrusData.print_text( 'Sending shut down instruction' + HC.UNICODE_ELLIPSIS )
            
            r = session.post( 'https://127.0.0.1:' + str( port ) + '/shutdown' )
            
            if not r.ok:
                
                text = 'When told to shut down, the existing server gave an error!'
                text += '\n'
                text += r.text
                
                raise HydrusExceptions.ShutdownException( text )
                
            
            time_waited = 0
            
            while HydrusProcess.is_already_running( db_dir, 'server' ):
                
                time.sleep( 1 )
                
                time_waited += 1
                
                if time_waited > 20:
                    
                    raise HydrusExceptions.ShutdownException( 'Attempted to shut the existing server down, but it took too long!' )
                    
                
            
            break
            
        
    
    if not port_found:
        
        raise HydrusExceptions.ShutdownException( 'The existing server did not have an administration service!' )
        
    
    HydrusData.print_text( 'The existing server is shut down!' )
    

class Controller( HydrusController.HydrusController ):
    
    def __init__( self, db_dir, logger ):
        
        super().__init__( db_dir, logger )
        
        self._name = 'server'
        
        self._shutdown = False
        
        SG.server_controller = self
        
        self.call_to_thread_long_running( self.daemon_pub_sub )
        
    
    def _get_upnp_services( self ):
        
        return self._services
        
    
    def _init_db( self ):
        
        self.db = ServerDB.DB( self, self.db_dir, 'server' )
        
    
    def daemon_pub_sub( self ):
        
        while not HG.model_shutdown:
            
            if self._pubsub.work_to_do():
                
                try:
                    
                    self._pubsub.process()
                    
                except Exception as e:
                    
                    HydrusData.ShowException( e, do_wait = True )
                    
                
            else:
                
                self._pubsub.wait_on_pub()
                
            
        
    
    def do_deferred_physical_deletes( self ):
        
        num_files_deleted = 0
        num_thumbnails_deleted = 0
        
        pauser = HydrusThreading.BigJobPauser()
        
        ( file_hash, thumbnail_hash ) = self.read( 'deferred_physical_delete' )
        
        while ( file_hash is not None or thumbnail_hash is not None ) and not HG.started_shutdown:
            
            if file_hash is not None:
                
                path = ServerFiles.get_expected_file_path( file_hash )
                
                if os.path.exists( path ):
                    
                    HydrusPaths.recycle_path( path )
                    
                    num_files_deleted += 1
                    
                
            
            if thumbnail_hash is not None:
                
                path = ServerFiles.get_expected_thumbnail_path( thumbnail_hash )
                
                if os.path.exists( path ):
                    
                    HydrusPaths.recycle_path( path )
                    
                    num_thumbnails_deleted += 1
                    
                
            
            self.write_synchronous( 'clear_deferred_physical_delete', file_hash = file_hash, thumbnail_hash = thumbnail_hash )
            
            ( file_hash, thumbnail_hash ) = self.read( 'deferred_physical_delete' )
            
            pauser.pause()
            
        
        if num_files_deleted > 0 or num_thumbnails_deleted > 0:
            
            HydrusData.print_text( 'Physically deleted {} files and {} thumbnails from file storage.'.format( HydrusNumbers.to_human_int( num_files_deleted ), HydrusNumbers.to_human_int( num_files_deleted ) ) )
            
        
    
    def exit( self ):
        
        HG.started_shutdown = True
        
        self.save_dirty_objects()
        
        HydrusData.print_text( 'Shutting down daemons' + HC.UNICODE_ELLIPSIS )
        
        self.shutdown_view()
        
        HydrusData.print_text( 'Shutting down db' + HC.UNICODE_ELLIPSIS )
        
        self.shutdown_model()
        
        self.clean_running_file()
        
    
    def get_files_dir( self ):
        
        return self.db.get_files_dir()
        
    
    def get_services( self ):
        
        return list( self._services )
        
    
    def init_model( self ):
        
        HydrusController.HydrusController.init_model( self )
        
        self._services = self.read( 'services' )
        
        [ self._admin_service ] = [service for service in self._services if service.get_service_type() == HC.SERVER_ADMIN]
        
        self.server_session_manager = HydrusSessions.HydrusSessionManagerServer()
        
        self._service_keys_to_connected_ports = {}
        
    
    def init_view( self ):
        
        HydrusController.HydrusController.init_view( self )
        
        port = self._admin_service.get_port()
        
        if HydrusNetworking.local_port_in_use( port ):
            
            HydrusData.print_text( 'Something is already bound to port ' + str( port ) + ', so your administration service cannot be started. Please quit the server and retry once the port is clear.' )
            
        else:
            
            self.restart_services()
            
        
        #
        
        job = self.call_repeating( 5.0, HydrusNetwork.UPDATE_CHECKING_PERIOD, self.sync_repositories )
        job.wake_on_pub_sub( 'notify_new_repo_sync' )
        
        self._daemon_jobs[ 'sync_repositories' ] = job
        
        job = self.call_repeating( 0.0, 30.0, self.save_dirty_objects )
        
        self._daemon_jobs[ 'save_dirty_objects' ] = job
        
        job = self.call_repeating( 30.0, 86400.0, self.do_deferred_physical_deletes )
        job.wake_on_pub_sub( 'notify_new_physical_file_deletes' )
        
        self._daemon_jobs[ 'deferred_physical_deletes' ] = job
        
        job = self.call_repeating( 120.0, 3600.0 * 4, self.nullify_history )
        job.wake_on_pub_sub( 'notify_new_nullification' )
        
        self._daemon_jobs[ 'nullify_history' ] = job
        
    
    def just_woke_from_sleep( self ):
        
        return False
        
    
    def maintain_db( self, maintenance_mode = HC.MAINTENANCE_FORCED, stop_time = None ):
        
        stop_time = HydrusTime.get_now() + 10
        
        self.write_synchronous( 'analyze', maintenance_mode = maintenance_mode, stop_time = stop_time )
        
    
    def nullify_history( self ):
        
        repositories = [service for service in self._services if service.get_service_type() in HC.REPOSITORIES]
        
        for service in repositories:
            
            service.NullifyHistory()
            
        
    
    def report_data_used( self, num_bytes ):
        
        self._admin_service.ServerReportDataUsed( num_bytes )
        
    
    def report_request_used( self ):
        
        self._admin_service.ServerReportRequestUsed()
        
    
    def restart_services( self ):
        
        self.set_running_twisted_services( self._services )
        
    
    def run( self ):
        
        self.record_running_start()
        
        HydrusData.print_text( 'Initialising db' + HC.UNICODE_ELLIPSIS )
        
        self.init_model()
        
        HydrusData.print_text( 'Initialising workers' + HC.UNICODE_ELLIPSIS )
        
        self.init_view()
        
        HydrusData.print_text( 'Server is running. Press Ctrl+C to quit.' )
        
        try:
            
            while not HG.model_shutdown and not self._shutdown:
                
                time.sleep( 1 )
                
            
        except KeyboardInterrupt:
            
            HydrusData.print_text( 'Received a keyboard interrupt' + HC.UNICODE_ELLIPSIS )
            
        
        HydrusData.print_text( 'Shutting down controller' + HC.UNICODE_ELLIPSIS )
        
        self.exit()
        
    
    def save_dirty_objects( self ):
        
        with HG.dirty_object_lock:
            
            dirty_services = [service for service in self._services if service.is_dirty()]
            
            if len( dirty_services ) > 0:
                
                self.write_synchronous( 'dirty_services', dirty_services )
                
            
            dirty_accounts = self.server_session_manager.get_dirty_accounts()
            
            if len( dirty_accounts ) > 0:
                
                self.write_synchronous( 'dirty_accounts', dirty_accounts )
                
            
        
    
    def server_bandwidth_ok( self ):
        
        return self._admin_service.ServerBandwidthOK()
        
    
    def set_running_twisted_services( self, services ):
        
        def TWISTEDDoIt():
            
            def StartServices( *args, **kwargs ):
                
                HydrusData.print_text( 'Starting services' + HC.UNICODE_ELLIPSIS )
                
                for service in services:
                    
                    service_key = service.get_service_key()
                    service_type = service.get_service_type()
                    
                    name = service.get_name()
                    
                    try:
                        
                        port = service.get_port()
                        
                        if service_type == HC.SERVER_ADMIN:
                            
                            http_factory = ServerServer.HydrusServiceAdmin( service )
                            
                        elif service_type == HC.FILE_REPOSITORY:
                            
                            http_factory = ServerServer.HydrusServiceRepositoryFile( service )
                            
                        elif service_type == HC.TAG_REPOSITORY:
                            
                            http_factory = ServerServer.HydrusServiceRepositoryTag( service )
                            
                        else:
                            
                            return
                            
                        
                        from hydrus.core.networking import HydrusServerContextFactory
                        
                        ( ssl_cert_path, ssl_key_path ) = self.db.get_ssl_paths()
                        
                        context_factory = HydrusServerContextFactory.generate_ssl_context_factory( ssl_cert_path, ssl_key_path )
                        
                        ipv6_port = None
                        
                        try:
                            
                            ipv6_port = reactor.listenSSL( port, http_factory, context_factory, interface = '::' )
                            
                        except Exception as e:
                            
                            HydrusData.print_text( 'Could not bind to IPv6:' )
                            
                            HydrusData.print_text( str( e ) )
                            
                        
                        ipv4_port = None
                        
                        try:
                            
                            ipv4_port = reactor.listenSSL( port, http_factory, context_factory )
                            
                        except Exception as e:
                            
                            if ipv6_port is None:
                                
                                raise
                                
                            
                        
                        self._service_keys_to_connected_ports[ service_key ] = ( ipv4_port, ipv6_port )
                        
                        if HydrusNetworking.local_port_in_use( port ):
                            
                            HydrusData.print_text( 'Running "{}" on port {}.'.format( name, port ) )
                            
                        else:
                            
                            raise Exception( 'Tried to bind port {} for "{}" but it failed.'.format( port, name ) )
                            
                        
                    except Exception as e:
                        
                        HydrusData.print_text( traceback.format_exc() )
                        
                    
                
                HydrusData.print_text( 'Services started' )
                
            
            if len( self._service_keys_to_connected_ports ) > 0:
                
                HydrusData.print_text( 'Stopping services' + HC.UNICODE_ELLIPSIS )
                
                deferreds = []
                
                for ( ipv4_port, ipv6_port ) in self._service_keys_to_connected_ports.values():
                    
                    if ipv4_port is not None:
                        
                        deferred = defer.maybeDeferred( ipv4_port.stopListening )
                        
                        deferreds.append( deferred )
                        
                    
                    if ipv6_port is not None:
                        
                        deferred = defer.maybeDeferred( ipv6_port.stopListening )
                        
                        deferreds.append( deferred )
                        
                    
                
                self._service_keys_to_connected_ports = {}
                
                deferred = defer.DeferredList( deferreds )
                
                if len( services ) > 0:
                    
                    deferred.addCallback( StartServices )
                    
                
            elif len( services ) > 0:
                
                StartServices()
                
            
        
        threads.blockingCallFromThread( reactor, TWISTEDDoIt )
        
    
    def set_services( self, services ):
        
        # doesn't need the dirty_object_lock because the caller takes it
        
        # first test available ports
        
        my_ports = {s.get_port() for s in self._services}
        
        for service in services:
            
            port = service.get_port()
            
            if port not in my_ports and HydrusNetworking.local_port_in_use( port ):
                
                raise HydrusExceptions.ServerException( 'Something was already bound to port ' + str( port ) )
                
            
        
        #
        
        self._services = services
        
        self.call_to_thread( self.services_upnp_manager.set_services, self._services )
        
        [ self._admin_service ] = [service for service in self._services if service.get_service_type() == HC.SERVER_ADMIN]
        
        self.restart_services()
        
    
    def shutdown_view( self ):
        
        try:
            
            self.set_running_twisted_services( [] )
            
        except Exception as e:
            
            pass # sometimes this throws a wobbler, screw it
            
        
        HydrusController.HydrusController.shutdown_view( self )
        
    
    def shutdown_from_server( self ):
        
        HydrusData.print_text( 'Received a server shut down request' + HC.UNICODE_ELLIPSIS )
        
        self._shutdown = True
        
    
    def sync_repositories( self ):
        
        repositories = [service for service in self._services if service.get_service_type() in HC.REPOSITORIES]
        
        for service in repositories:
            
            service.Sync()
            
        
    
