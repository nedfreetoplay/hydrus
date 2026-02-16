import unittest

from hydrus.core import HydrusTime

class TestHydrusTime( unittest.TestCase ):
    
    def test_quick( self ):
        
        self.assertEqual(HydrusTime.time_delta_to_pretty_time_delta(86400 * 5), '5 days')
        self.assertIn( 'month', HydrusTime.time_delta_to_pretty_time_delta(86400 * 35))
        self.assertEqual(HydrusTime.time_delta_to_pretty_time_delta(86400 * 35, no_bigger_than_days = True), '35 days')
        
    
