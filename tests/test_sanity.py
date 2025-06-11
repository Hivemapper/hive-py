import unittest

class TestSanity(unittest.TestCase):
    def test_sanity(self):
        """Test 1 == 1"""
        self.assertEqual(1, 1)

if __name__ == '__main__':
    unittest.main()
