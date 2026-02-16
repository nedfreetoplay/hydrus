import unittest

from hydrus.core import HydrusTags
from hydrus.core import HydrusText

class TestHydrusTags( unittest.TestCase ):
    
    def test_cleaning_and_combining( self ):
        
        self.assertEqual(HydrusTags.clean_tag(' test '), 'test')
        self.assertEqual(HydrusTags.clean_tag(' character:test '), 'character:test')
        self.assertEqual(HydrusTags.clean_tag(' character : test '), 'character:test')
        
        self.assertEqual(HydrusTags.clean_tag(':p'), '::p')
        
        self.assertEqual(HydrusTags.combine_tag('', ':p'), '::p')
        self.assertEqual(HydrusTags.combine_tag('', '::p'), ':::p')
        
        self.assertEqual(HydrusTags.combine_tag('', 'unnamespace:withcolon'), ':unnamespace:withcolon')
        
    
    def test_latin_ok( self ):
        
        self.assertEqual(HydrusTags.clean_tag('Hello world!(){}[]*/-+'), 'hello world!(){}[]*/-+')
        
        self.assertEqual(HydrusTags.clean_tag('naïve Déjà vu'), 'naïve déjà vu')
        
    
    def test_latin_ok_zwj_zwnj( self ):
        
        self.assertEqual(HydrusTags.clean_tag('Hello world!\u200C(){}\u200D[]*/-+'), 'hello world!(){}[]*/-+')
        
        self.assertEqual(HydrusTags.clean_tag('naï\u200Cve Déjà \u200Dvu'), 'naïve déjà vu')
        
        self.assertEqual(HydrusTags.clean_tag(' \u200C\u200D '), '')
        
    
    def test_control_garbage( self ):
        
        self.assertEqual(HydrusTags.clean_tag('t\u0000est'), 'test')
        self.assertEqual(HydrusTags.clean_tag('t\u0001est'), 'test')
        self.assertEqual(HydrusTags.clean_tag('t\u009Fest'), 'test')
        self.assertEqual(HydrusTags.clean_tag('t\nest'), 'test')
        self.assertEqual(HydrusTags.clean_tag('t\test'), 'test')
        self.assertEqual(HydrusTags.clean_tag('t\rest'), 'test')
        
    
    def test_format_stuff( self ):
        
        self.assertEqual(HydrusTags.clean_tag('t\u200De\u200Cs\u200Bt\u200E'), 'test')
        
    
    def test_private_and_surrogates( self ):
        
        self.assertEqual(HydrusTags.clean_tag('t\uE000e\U000F0000s\uD834t'), 'test')
        
    
    def test_emoji_ZWJ( self ):
        
        # this makes 'family' through the magical power of 'graphemes', which are not codepoints
        self.assertEqual(HydrusTags.clean_tag('👨\u200D👩\u200D👦\u200D👧'), '👨\u200D👩\u200D👦\u200D👧')
        
    
    def test_hangul_detection( self ):
        
        self.assertEqual(HydrusTags.clean_tag('test\u3164'), 'test')
        self.assertEqual(HydrusTags.clean_tag('te:s:t\u3164'), 'te:s:t')
        self.assertEqual(HydrusTags.clean_tag('한글\u3164'), '한글\u3164')
        
    
    def test_surrogate_garbage( self ):
        
        # note this is a dangerous string bro and the debugger will freak out if you inspect it
        self.assertEqual(HydrusText.cleanse_import_text('test \ud83d\ude1c'), 'test \U0001f61c')
        
    
