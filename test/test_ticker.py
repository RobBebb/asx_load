import unittest
from dotenv import load_dotenv
import pandas as pd
from securities_load.load.postgresql_database_functions import connect
from asx_load.load.asx_functions import (get_ticker_id, get_gics_sector_code, 
    get_gics_industry_group_code, get_gics_industry_code, get_gics_sub_industry_code,
    get_ticker_id_using_yahoo_ticker, get_ticker_using_id)

class TestGetTicker(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
    
    def test_get_ticker_id_found(self):
        self.assertEqual(get_ticker_id(self.conn, 'ASX', 'BHP'), 250)
        
    def test_get_ticker_id_not_found(self):
        self.assertEqual(get_ticker_id(self.conn, 'ASX', 'ZZZ'), None)

    def tearDown(self):
        self.conn.close()

class TestGetTickerUsingYahooTicker(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
    
    def test_get_ticker_id_found(self):
        self.assertEqual(get_ticker_id_using_yahoo_ticker(self.conn, 'BHP.AX'), 250)
        
    def test_get_ticker_id_not_found(self):
        self.assertEqual(get_ticker_id_using_yahoo_ticker(self.conn, 'ZZZ.AX'), None)

    def tearDown(self):
        self.conn.close()

class TestGetTickerUsingTickerId(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
    
    def test_get_ticker_using_ticker_found(self):
        self.assertEqual(get_ticker_using_id(self.conn, 250), ('BHP', 'ASX'))
        
    def test_get_ticker_id_using_ticker_not_found(self):
        self.assertEqual(get_ticker_using_id(self.conn, 888888), None)

    def tearDown(self):
        self.conn.close()



class TestGetGicsSectorCode(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
    
    def test_get_gics_sector_code_found(self):
        self.assertEqual(get_gics_sector_code(self.conn, 'Industrials'), '20') 
           
    def test_get_gics_sector_code_not_found(self):
        self.assertEqual(get_gics_sector_code(self.conn, 'Zzzzz'), None)

    def tearDown(self):
        self.conn.close()

class TestGetGicsIndustryGroupCode(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
    
    def test_get_gics_industry_group_code_found(self):
        self.assertEqual(get_gics_industry_group_code(self.conn, 'Capital Goods'), '2010') 
           
    def test_get_gics_industry_group_code_not_found(self):
        self.assertEqual(get_gics_industry_group_code(self.conn, 'Zzzzz'), None)

    def tearDown(self):
        self.conn.close()

class TestGetGicsIndustryCode(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
    
    def test_get_gics_industry_code_found(self):
        self.assertEqual(get_gics_industry_code(self.conn, 'Electrical Equipment'), '201040') 
           
    def test_get_gics_industry_code_not_found(self):
        self.assertEqual(get_gics_industry_code(self.conn, 'Zzzzz'), None)

    def tearDown(self):
        self.conn.close()

class TestGetGicsSubIndustryCode(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
    
    def test_get_gics_sub_industry_code_found(self):
        self.assertEqual(get_gics_sub_industry_code(self.conn, 'Electrical Components & Equipment'), '20104010') 
           
    def test_get_gics_sub_industry_code_not_found(self):
        self.assertEqual(get_gics_sub_industry_code(self.conn, 'Zzzzz'), None)

    def tearDown(self):
        self.conn.close()

if __name__ == '__main__':
    load_dotenv()

    # vervosity: 0 for quiet, 1 for normal, 2 for detailed
    unittest.main(verbosity=1)