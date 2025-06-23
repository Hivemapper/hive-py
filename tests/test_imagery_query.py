import unittest
import os
import json
import shutil
from datetime import datetime

from tests.fixtures.fixture_feature import test_feature
from imagery import query

class TestImageryQuery(unittest.TestCase):

    def setUp(self):
        self.test_feature = json.loads(test_feature)
        # write test_feature to file
        with open('test_feature.json', 'w') as f:
            json.dump(self.test_feature, f)
        
        # Create cache directory if it doesn't exist
        os.makedirs('.hivepy_cache', exist_ok=True)

    def test_query(self):
        """Test for 'query'"""
        auth = os.getenv('HIVE_PY_UNIT_TEST_AUTH')
        if not auth:
            raise ValueError('HIVE_PY_UNIT_TEST_AUTH environment variable is not set')
        
        start_date = datetime.strptime('2025-01-01', '%Y-%m-%d')
        end_date = datetime.strptime('2025-01-02', '%Y-%m-%d')
        
        frames = query('test_feature.json', start_date, end_date, 'output', auth, use_cache=False)
        self.assertEqual(len(frames), 23)

    def tearDown(self):
        os.remove('test_feature.json')
        # Clean up cache directory
        if os.path.exists('.hivepy_cache'):
            shutil.rmtree('.hivepy_cache')

if __name__ == '__main__':
    unittest.main()
