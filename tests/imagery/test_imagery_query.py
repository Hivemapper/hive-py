import unittest
import os

class TestImageryQuery(unittest.TestCase):
    def test_query_frames(self):
        """Test for 'query_frames'"""
        auth = os.getenv('HIVE_PY_UNIT_TEST_AUTH')
        if not auth:
            raise ValueError('HIVE_PY_UNIT_TEST_AUTH environment variable is not set')
        #todo mtp

if __name__ == '__main__':
    unittest.main()
