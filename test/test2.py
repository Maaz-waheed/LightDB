import os
import unittest
import random
import string

from maazDB import maazDB


class TestDB(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Ensure no existing files interfere."""
        for i in range(1, 100):
            filename = f"file_{i}.dm"
            if os.path.exists(filename):
                os.remove(filename)

    def test_create_files_with_data(self):
        """Create 99 .dm files with 100 rows and 29 columns of random data."""
        for i in range(1, 100):
            db = maazDB(filename=f"file_{i}.dm")
            for j in range(1, 101):
                row_data = {
                    f"col_{k}": ''.join(random.choices(string.ascii_letters, k=5))
                    for k in range(1, 30)
                }
                db.insert(f"row_{j}", row_data)

            # Assert the file was created
            self.assertTrue(os.path.exists(f"file_{i}.dm"))

    @classmethod
    def tearDownClass(cls):
        #pass
        """Clean up after tests."""
        for i in range(1, 100):
            filename = f"file_{i}.dm"
            if os.path.exists(filename):
                os.remove(filename)


if __name__ == '__main__':
    unittest.main()
