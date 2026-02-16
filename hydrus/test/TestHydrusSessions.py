import hashlib
import unittest

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusSessions
from hydrus.core import HydrusTime
from hydrus.core.networking import HydrusNetwork

from hydrus.test import TestGlobals as TG

class TestSessions( unittest.TestCase ):
    
    def test_server( self ):
        
        discard = TG.test_controller.GetWrite( 'session' ) # just to discard gumph from testserver
        
        session_key_1 = HydrusData.generate_key()
        service_key = HydrusData.generate_key()
        
        permissions = [ HC.GET_DATA, HC.POST_DATA, HC.POST_PETITIONS, HC.RESOLVE_PETITIONS, HC.MANAGE_USERS, HC.GENERAL_ADMIN, HC.EDIT_SERVICES ]
        
        account_type = HydrusNetwork.AccountType.generate_admin_account_type(HC.SERVER_ADMIN)
        created = HydrusTime.get_now() - 100000
        expires = HydrusTime.get_now() + 300
        
        account_key_1 = HydrusData.generate_key()
        account_key_2 = HydrusData.generate_key()
        
        access_key_1 = HydrusData.generate_key()
        hashed_access_key_1 = hashlib.sha256( access_key_1 ).digest()
        
        access_key_2 = HydrusData.generate_key()
        hashed_access_key_2 = hashlib.sha256( access_key_2 ).digest()
        
        account = HydrusNetwork.Account( account_key_1, account_type, created, expires )
        account_2 = HydrusNetwork.Account( account_key_2, account_type, created, expires )
        
        # test timeout
        
        expires = HydrusTime.get_now() - 10
        
        TG.test_controller.SetRead( 'sessions', [ ( session_key_1, service_key, account, hashed_access_key_1, expires ) ] )
        
        session_manager = HydrusSessions.HydrusSessionManagerServer()
        
        with self.assertRaises( HydrusExceptions.SessionException ):
            
            session_manager.get_account(service_key, session_key_1)
            
        
        # test missing
        
        with self.assertRaises( HydrusExceptions.SessionException ):
            
            session_manager.get_account(service_key, HydrusData.generate_key())
            
        
        # test fetching a session already in db, after bootup
        
        expires = HydrusTime.get_now() + 300
        
        TG.test_controller.SetRead( 'sessions', [ ( session_key_1, service_key, account, hashed_access_key_1, expires ) ] )
        
        session_manager = HydrusSessions.HydrusSessionManagerServer()
        
        read_account = session_manager.get_account(service_key, session_key_1)
        
        self.assertIs( read_account, account )
        
        read_account = session_manager.get_account_from_access_key(service_key, access_key_1)
        
        self.assertIs( read_account, account )
        
        # test too busy to add a new session for a new account it doesn't know about
        
        HG.server_busy.acquire()
        
        with self.assertRaises( HydrusExceptions.ServerBusyException ):
            
            session_manager.add_session(service_key, HydrusData.generate_key())
            
            session_manager.get_account_from_access_key(service_key, HydrusData.generate_key())
            
        
        # but ok to get for a session that already exists while busy
        
        session_manager.get_account(service_key, session_key_1)
        session_manager.get_account_from_access_key(service_key, access_key_1)
        
        HG.server_busy.release()
        
        # test adding a session
        
        TG.test_controller.ClearWrites( 'session' )
        
        expires = HydrusTime.get_now() + 300
        
        TG.test_controller.SetRead( 'account_key_from_access_key', account_key_2 )
        TG.test_controller.SetRead( 'account', account_2 )
        
        ( session_key_2, expires_2 ) = session_manager.add_session(service_key, access_key_2)
        
        [ ( args, kwargs ) ] = TG.test_controller.GetWrite( 'session' )
        
        ( written_session_key, written_service_key, written_account_key, written_expires ) = args
        
        self.assertEqual( ( session_key_2, service_key, account_key_2, expires_2 ), ( written_session_key, written_service_key, written_account_key, written_expires ) )
        
        read_account = session_manager.get_account(service_key, session_key_2)
        
        self.assertIs( read_account, account_2 )
        
        read_account = session_manager.get_account_from_access_key(service_key, access_key_2)
        
        self.assertIs( read_account, account_2 )
        
        # test adding a new session for an account already in the manager
        
        TG.test_controller.SetRead( 'account_key_from_access_key', account_key_1 )
        TG.test_controller.SetRead( 'account', account )
        
        ( session_key_3, expires_3 ) = session_manager.add_session(service_key, access_key_1)
        
        [ ( args, kwargs ) ] = TG.test_controller.GetWrite( 'session' )
        
        ( written_session_key, written_service_key, written_account_key, written_expires ) = args
        
        self.assertEqual( ( session_key_3, service_key, account_key_1, expires_3 ), ( written_session_key, written_service_key, written_account_key, written_expires ) )
        
        read_account = session_manager.get_account(service_key, session_key_1)
        
        self.assertIs( read_account, account )
        
        read_account = session_manager.get_account(service_key, session_key_3)
        
        self.assertIs( read_account, account )
        
        read_account = session_manager.get_account_from_access_key(service_key, access_key_1)
        
        self.assertIs( read_account, account )
        
        # test individual account refresh
        
        expires = HydrusTime.get_now() + 300
        
        new_obj_account_1 = HydrusNetwork.Account( account_key_1, account_type, created, expires )
        
        TG.test_controller.SetRead( 'account', new_obj_account_1 )
        
        session_manager.refresh_accounts(service_key, [account_key_1])
        
        read_account = session_manager.get_account(service_key, session_key_1)
        
        self.assertIs( read_account, new_obj_account_1 )
        
        read_account = session_manager.get_account(service_key, session_key_3)
        
        self.assertIs( read_account, new_obj_account_1 )
        
        read_account = session_manager.get_account_from_access_key(service_key, access_key_1)
        
        self.assertIs( read_account, new_obj_account_1 )
        
        # test all account refresh
        
        expires = HydrusTime.get_now() + 300
        
        new_obj_account_2 = HydrusNetwork.Account( account_key_2, account_type, created, expires )
        
        TG.test_controller.SetRead( 'sessions', [ ( session_key_1, service_key, new_obj_account_2, hashed_access_key_2, expires ), ( session_key_2, service_key, new_obj_account_1, hashed_access_key_1, expires ), ( session_key_3, service_key, new_obj_account_2, hashed_access_key_2, expires ) ] )
        
        session_manager.refresh_all_accounts()
        
        read_account = session_manager.get_account(service_key, session_key_1)
        
        self.assertIs( read_account, new_obj_account_2 )
        
        read_account = session_manager.get_account(service_key, session_key_2)
        
        self.assertIs( read_account, new_obj_account_1 )
        
        read_account = session_manager.get_account(service_key, session_key_3)
        
        self.assertIs( read_account, new_obj_account_2 )
        
        read_account = session_manager.get_account_from_access_key(service_key, access_key_1)
        
        self.assertIs( read_account, new_obj_account_1 )
        
        read_account = session_manager.get_account_from_access_key(service_key, access_key_2)
        
        self.assertIs( read_account, new_obj_account_2 )
        
