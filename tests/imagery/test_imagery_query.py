import unittest
import os

from imagery import query_frames

class TestImageryQuery(unittest.TestCase):
    def test_query_frames(self):
        """Test for 'query_frames'"""
        auth = os.getenv('HIVE_PY_UNIT_TEST_AUTH')
        if not auth:
            raise ValueError('HIVE_PY_UNIT_TEST_AUTH environment variable is not set')
        frames = query_frames('tests/imagery/test_data/test_feature.json', '2025-01-01', '2025-01-02', 'tests/imagery/test_data/output', auth)
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()
