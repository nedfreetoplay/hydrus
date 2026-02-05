import threading
import time

from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core.networking import HydrusServerRequest
from hydrus.core.networking import HydrusServerResources

from hydrus.client import ClientAPI
from hydrus.client import ClientConstants as CC
from hydrus.client import ClientGlobals as CG
from hydrus.client import ClientLocation
from hydrus.client import ClientOptions
from hydrus.client import ClientThreading
from hydrus.client.networking.api import ClientLocalServerCore
from hydrus.client.networking.api import ClientLocalServerResources
from hydrus.client.search import ClientSearchFileSearchContext
from hydrus.client.search import ClientSearchTagContext

class HydrusResourceClientAPIRestrictedManageDatabase( ClientLocalServerResources.HydrusResourceClientAPIRestricted ):
    
    def _CheckAPIPermissions( self, request: HydrusServerRequest.HydrusRequest ):
        
        request.client_api_permissions.CheckPermission( ClientAPI.CLIENT_API_PERMISSION_MANAGE_DATABASE )
        
    

class HydrusResourceClientAPIRestrictedManageDatabaseForceCommit( HydrusResourceClientAPIRestrictedManageDatabase ):
    
    def _threadDoPOSTJob( self, request: HydrusServerRequest.HydrusRequest ):
        
        CG.client_controller.db.force_a_commit()
        
        response_context = HydrusServerResources.ResponseContext( 200 )
        
        return response_context
        
    

class HydrusResourceClientAPIRestrictedManageDatabaseLockOff( HydrusResourceClientAPIRestrictedManageDatabase ):
    
    BLOCKED_WHEN_BUSY = False
    
    def _threadDoPOSTJob( self, request: HydrusServerRequest.HydrusRequest ):
        
        try:
            
            HG.client_busy.release()
            
        except threading.ThreadError:
            
            raise HydrusExceptions.BadRequestException( 'The server is not busy!' )
            
        
        CG.client_controller.db.pause_and_disconnect( False )
        
        response_context = HydrusServerResources.ResponseContext( 200 )
        
        return response_context
        
    

class HydrusResourceClientAPIRestrictedManageDatabaseLockOn( HydrusResourceClientAPIRestrictedManageDatabase ):
    
    def _threadDoPOSTJob( self, request: HydrusServerRequest.HydrusRequest ):
        
        locked = HG.client_busy.acquire( False ) # pylint: disable=E1111
        
        if not locked:
            
            raise HydrusExceptions.BadRequestException( 'The client was already locked!' )
            
        
        CG.client_controller.db.pause_and_disconnect( True )
        
        TIME_BLOCK = 0.25
        
        for i in range( int( 5 / TIME_BLOCK ) ):
            
            if not CG.client_controller.db.is_connected():
                
                break
                
            
            time.sleep( TIME_BLOCK )
            
        
        response_context = HydrusServerResources.ResponseContext( 200 )
        
        return response_context
        
    

class HydrusResourceClientAPIRestrictedManageDatabaseMrBones( HydrusResourceClientAPIRestrictedManageDatabase ):
    
    def _threadDoGETJob( self, request: HydrusServerRequest.HydrusRequest ):
        
        location_context = ClientLocalServerCore.ParseLocationContext(request, ClientLocation.LocationContext.static_create_simple(CC.COMBINED_LOCAL_FILE_DOMAINS_SERVICE_KEY))
        
        tag_service_key = ClientLocalServerCore.ParseTagServiceKey( request )
        
        if tag_service_key == CC.COMBINED_TAG_SERVICE_KEY and location_context.is_all_known_files():
            
            raise HydrusExceptions.BadRequestException( 'Sorry, search for all known tags over all known files is not supported!' )
            
        
        tag_context = ClientSearchTagContext.TagContext( service_key = tag_service_key )
        predicates = ClientLocalServerCore.ParseClientAPISearchPredicates( request )
        
        file_search_context = ClientSearchFileSearchContext.FileSearchContext( location_context = location_context, tag_context = tag_context, predicates = predicates )
        
        job_status = ClientThreading.JobStatus( cancellable = True )
        
        request.disconnect_callables.append(job_status.cancel)
        
        boned_stats = CG.client_controller.read( 'boned_stats', file_search_context = file_search_context, job_status = job_status )
        
        body_dict = { 'boned_stats' : boned_stats }
        
        mime = request.preferred_mime
        body = ClientLocalServerCore.Dumps( body_dict, mime )
        
        response_context = HydrusServerResources.ResponseContext( 200, mime = mime, body = body )
        
        return response_context
        
    

class HydrusResourceClientAPIRestrictedManageDatabaseGetClientOptions( HydrusResourceClientAPIRestrictedManageDatabase ):
    
    def _threadDoGETJob( self, request: HydrusServerRequest.HydrusRequest ):
        
        from hydrus.client import ClientDefaults
        
        OLD_OPTIONS_DEFAULT = ClientDefaults.get_client_default_options()
        
        old_options = CG.client_controller.options
        
        old_options = { key : value for ( key, value ) in old_options.items() if key in OLD_OPTIONS_DEFAULT }
        
        new_options: ClientOptions.ClientOptions = CG.client_controller.new_options

        options_dict = {
            'booleans' : new_options.get_all_booleans(),
            'strings' : new_options.get_all_strings(),
            'noneable_strings' : new_options.get_all_noneable_strings(),
            'integers' : new_options.get_all_integers(),
            'noneable_integers' : new_options.get_all_noneable_integers(),
            'keys' : new_options.get_all_keys_hex(),
            'colors' : new_options.get_all_colours(),
            'media_zooms' : new_options.get_media_zooms(),
            'slideshow_durations' : new_options.get_slideshow_durations(),
            'default_file_import_options' : {
                'loud' : new_options.get_default_file_import_options('loud').GetSummary(),
                'quiet' : new_options.get_default_file_import_options('quiet').GetSummary()
            },
            'default_namespace_sorts' : [sort.to_dict_for_api() for sort in new_options.get_default_namespace_sorts()],
            'default_sort' : new_options.get_default_sort().to_dict_for_api(),
            'default_tag_sort' : new_options.get_default_tag_sort(CC.TAG_PRESENTATION_SEARCH_PAGE).to_dict_for_api(),
            'default_tag_sort_search_page' : new_options.get_default_tag_sort(CC.TAG_PRESENTATION_SEARCH_PAGE).to_dict_for_api(),
            'default_tag_sort_search_page_manage_tags' : new_options.get_default_tag_sort(CC.TAG_PRESENTATION_SEARCH_PAGE_MANAGE_TAGS).to_dict_for_api(),
            'default_tag_sort_media_viewer' : new_options.get_default_tag_sort(CC.TAG_PRESENTATION_MEDIA_VIEWER).to_dict_for_api(),
            'default_tag_sort_media_vewier_manage_tags' : new_options.get_default_tag_sort(CC.TAG_PRESENTATION_MEDIA_VIEWER_MANAGE_TAGS).to_dict_for_api(),
            'fallback_sort' : new_options.get_fallback_sort().to_dict_for_api(),
            'suggested_tags_favourites' : new_options.get_all_suggested_tags_favourites(),
            'default_local_location_context' : new_options.get_default_local_location_context().to_dict_for_api()
        }

        body_dict = {
            'old_options' : old_options,
            'options' : options_dict,
            'services' : ClientLocalServerCore.GetServicesDict()
        }
        
        body = ClientLocalServerCore.Dumps( body_dict, request.preferred_mime )
        
        response_context = HydrusServerResources.ResponseContext( 200, mime = request.preferred_mime, body = body )
        
        return response_context
        
    
