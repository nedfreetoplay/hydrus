import unittest

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData

from hydrus.client import ClientConstants as CC
from hydrus.client import ClientGlobals as CG
from hydrus.client import ClientManagers
from hydrus.client import ClientServices
from hydrus.client.metadata import ClientContentUpdates

from hydrus.test import HelperFunctions as HF
from hydrus.test import TestGlobals as TG

class TestManagers( unittest.TestCase ):
    
    def test_services( self ):
        
        def test_service( service, key, service_type, name ):
            
            self.assertEqual(service.get_service_key(), key)
            self.assertEqual(service.get_service_type(), service_type)
            self.assertEqual(service.get_name(), name)
            
        
        repo_key = HydrusData.generate_key()
        repo_type = HC.TAG_REPOSITORY
        repo_name = 'test tag repo'
        
        repo = ClientServices.generate_service(repo_key, repo_type, repo_name)
        
        services = []
        
        services.append( repo )
        
        TG.test_controller.SetRead( 'services', services )
        
        services_manager = ClientServices.ServicesManager( CG.client_controller )
        
        #
        
        service = services_manager.get_service(repo_key)
        
        test_service( service, repo_key, repo_type, repo_name )
        
        #
        
        services = services_manager.get_services((HC.TAG_REPOSITORY,))
        
        self.assertEqual( len( services ), 1 )
        
        self.assertEqual(services[0].get_service_key(), repo_key)
        
        #
        
        services = []
        
        TG.test_controller.SetRead( 'services', services )
        
        services_manager.refresh_services()
        
        self.assertRaises(Exception, services_manager.get_service, repo_key)
        
    
    def test_undo( self ):
        
        hash_1 = HydrusData.generate_key()
        hash_2 = HydrusData.generate_key()
        hash_3 = HydrusData.generate_key()
        
        command_1 = ClientContentUpdates.ContentUpdatePackage.STATICCreateFromContentUpdate( CC.HYDRUS_LOCAL_FILE_STORAGE_SERVICE_KEY, ClientContentUpdates.ContentUpdate( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_ARCHIVE, { hash_1 } ) )
        command_2 = ClientContentUpdates.ContentUpdatePackage.STATICCreateFromContentUpdate( CC.HYDRUS_LOCAL_FILE_STORAGE_SERVICE_KEY, ClientContentUpdates.ContentUpdate( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_INBOX, { hash_2 } ) )
        command_3 = ClientContentUpdates.ContentUpdatePackage.STATICCreateFromContentUpdate( CC.HYDRUS_LOCAL_FILE_STORAGE_SERVICE_KEY, ClientContentUpdates.ContentUpdate( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_ARCHIVE, { hash_1, hash_3 } ) )
        
        command_1_inverted = ClientContentUpdates.ContentUpdatePackage.STATICCreateFromContentUpdate( CC.HYDRUS_LOCAL_FILE_STORAGE_SERVICE_KEY, ClientContentUpdates.ContentUpdate( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_INBOX, { hash_1 } ) )
        command_2_inverted = ClientContentUpdates.ContentUpdatePackage.STATICCreateFromContentUpdate( CC.HYDRUS_LOCAL_FILE_STORAGE_SERVICE_KEY, ClientContentUpdates.ContentUpdate( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_ARCHIVE, { hash_2 } ) )
        
        undo_manager = ClientManagers.UndoManager( CG.client_controller )
        
        #
        
        TG.test_controller.ClearWrites( 'content_updates' )
        
        undo_manager.add_command('content_updates', command_1)
        
        self.assertEqual(( 'undo archive 1 files', None ), undo_manager.get_undo_redo_strings())
        
        undo_manager.add_command('content_updates', command_2)
        
        self.assertEqual(( 'undo inbox 1 files', None ), undo_manager.get_undo_redo_strings())
        
        undo_manager.undo()
        
        self.assertEqual(( 'undo archive 1 files', 'redo inbox 1 files' ), undo_manager.get_undo_redo_strings())
        
        [ ( ( content_update_package, ), kwargs ) ] = TG.test_controller.GetWrite( 'content_updates' )
        
        HF.compare_content_update_packages( self, content_update_package, command_2_inverted )
        
        undo_manager.redo()
        
        [ ( ( content_update_package, ), kwargs ) ] = TG.test_controller.GetWrite( 'content_updates' )
        
        HF.compare_content_update_packages( self, content_update_package, command_2 )
        
        self.assertEqual(( 'undo inbox 1 files', None ), undo_manager.get_undo_redo_strings())
        
        undo_manager.undo()
        
        [ ( ( content_update_package, ), kwargs ) ] = TG.test_controller.GetWrite( 'content_updates' )
        
        HF.compare_content_update_packages( self, content_update_package, command_2_inverted )
        
        undo_manager.undo()
        
        [ ( ( content_update_package, ), kwargs ) ] = TG.test_controller.GetWrite( 'content_updates' )
        
        HF.compare_content_update_packages( self, content_update_package, command_1_inverted )
        
        self.assertEqual(( None, 'redo archive 1 files' ), undo_manager.get_undo_redo_strings())
        
        undo_manager.add_command('content_updates', command_3)
        
        self.assertEqual(( 'undo archive 2 files', None ), undo_manager.get_undo_redo_strings())
        
    
