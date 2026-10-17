import random
import unittest

from unittest import mock

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusTime
from hydrus.core.networking import HydrusNetworking

now = HydrusTime.get_now()

now_10 = now + 10

now_20 = now + 20

with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
    
    HIGH_USAGE = HydrusNetworking.BandwidthTracker()
    
    for i in range( 100 ):
        
        HIGH_USAGE.report_request_used()
        HIGH_USAGE.report_data_used( random.randint( 512, 1024 ) )
        
    
    LOW_USAGE = HydrusNetworking.BandwidthTracker()
    
    LOW_USAGE.report_request_used()
    LOW_USAGE.report_data_used( 1024 )
    
    ZERO_USAGE = HydrusNetworking.BandwidthTracker()
    
class TestBandwidthRules( unittest.TestCase ):
    
    def test_no_rules( self ):
        
        rules = HydrusNetworking.BandwidthRules()
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertTrue( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
    
    def test_per_sec( self ):
        
        # at short time deltas, we can always start based on data alone
        
        rules = HydrusNetworking.BandwidthRules()
        
        rules.add_rule( HC.BANDWIDTH_TYPE_DATA, 1, 10240 )
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertTrue( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertFalse( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_10 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertTrue( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_20 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertTrue( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        #
        
        rules = HydrusNetworking.BandwidthRules()
        
        rules.add_rule( HC.BANDWIDTH_TYPE_REQUESTS, 1, 1 )
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertFalse( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_10 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertTrue( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_20 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertTrue( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        #
        
        rules = HydrusNetworking.BandwidthRules()
        
        rules.add_rule( HC.BANDWIDTH_TYPE_DATA, 1, 10240 )
        rules.add_rule( HC.BANDWIDTH_TYPE_REQUESTS, 1, 1 )
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertFalse( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertFalse( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_10 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertTrue( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_20 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertTrue( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
    
    def test_per_min( self ):
        
        # cutoff is 15s for continue
        
        rules = HydrusNetworking.BandwidthRules()
        
        rules.add_rule( HC.BANDWIDTH_TYPE_DATA, 60, 10240 )
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_10 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_20 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        #
        
        rules = HydrusNetworking.BandwidthRules()
        
        rules.add_rule( HC.BANDWIDTH_TYPE_REQUESTS, 60, 10 )
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_10 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_20 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        #
        
        rules = HydrusNetworking.BandwidthRules()
        
        rules.add_rule( HC.BANDWIDTH_TYPE_DATA, 60, 10240 )
        rules.add_rule( HC.BANDWIDTH_TYPE_REQUESTS, 60, 10 )
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_10 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_20 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
    
    def test_per_month( self ):
        
        rules = HydrusNetworking.BandwidthRules()
        
        rules.add_rule( HC.BANDWIDTH_TYPE_DATA, None, 10240 )
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_10 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_20 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        #
        
        rules = HydrusNetworking.BandwidthRules()
        
        rules.add_rule( HC.BANDWIDTH_TYPE_REQUESTS, None, 10 )
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_10 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_20 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        #
        
        rules = HydrusNetworking.BandwidthRules()
        
        rules.add_rule( HC.BANDWIDTH_TYPE_DATA, None, 10240 )
        rules.add_rule( HC.BANDWIDTH_TYPE_REQUESTS, None, 10 )
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_10 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now_20 ):
            
            self.assertTrue( rules.can_start_request( ZERO_USAGE ) )
            self.assertTrue( rules.can_start_request( LOW_USAGE ) )
            self.assertFalse( rules.can_start_request( HIGH_USAGE ) )
            
            self.assertTrue( rules.can_continue_download( ZERO_USAGE ) )
            self.assertTrue( rules.can_continue_download( LOW_USAGE ) )
            self.assertTrue( rules.can_continue_download( HIGH_USAGE ) )
            
        
    
class TestBandwidthTracker( unittest.TestCase ):
    
    def test_bandwidth_tracker( self ):
        
        bandwidth_tracker = HydrusNetworking.BandwidthTracker()
        
        self.assertEqual( bandwidth_tracker.get_current_month_summary(), 'used 0B in 0 requests this month' )
        
        now = HydrusTime.get_now()
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = now ):
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 0 ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 0 ), 0 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 1 ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 1 ), 0 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 2 ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 2 ), 0 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 6 ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 6 ), 0 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 3600 ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 3600 ), 0 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, None ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, None ), 0 )
            
            #
            
            bandwidth_tracker.report_data_used( 1024 )
            bandwidth_tracker.report_request_used()
            
            self.assertEqual( bandwidth_tracker.get_current_month_summary(), 'used 1 KB in 1 requests this month' )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 0 ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 0 ), 0 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 1 ), 1024 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 1 ), 1 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 2 ), 1024 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 2 ), 1 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 6 ), 1024 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 6 ), 1 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 3600 ), 1024 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 3600 ), 1 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, None ), 1024 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, None ), 1 )
            
        
        #
        
        five_secs_from_now = now + 5
        
        with mock.patch.object( HydrusTime, 'GetNow', return_value = five_secs_from_now ):
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 0 ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 0 ), 0 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 1 ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 1 ), 0 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 2 ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 2 ), 0 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 6 ), 1024 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 6 ), 1 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 3600 ), 1024 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 3600 ), 1 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, None ), 1024 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, None ), 1 )
            
            #
            
            bandwidth_tracker.report_data_used( 32 )
            bandwidth_tracker.report_request_used()
            
            bandwidth_tracker.report_data_used( 32 )
            bandwidth_tracker.report_request_used()
            
            self.assertEqual( bandwidth_tracker.get_current_month_summary(), 'used 1.06 KB in 3 requests this month' )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 0 ), 0 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 0 ), 0 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 1 ), 64 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 1 ), 2 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 2 ), 64 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 2 ), 2 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 6 ), 1088 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 6 ), 3 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, 3600 ), 1088 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, 3600 ), 3 )
            
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_DATA, None ), 1088 )
            self.assertEqual( bandwidth_tracker.get_usage( HC.BANDWIDTH_TYPE_REQUESTS, None ), 3 )
            
        
    
