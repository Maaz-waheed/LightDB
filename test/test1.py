import unittest
import os

from maazDB import maazDB

class TestMaazDB(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up a temporary database file for testing"""
        print("Setting up the test database...")
        cls.db = maazDB(filename="test_database.dm")

    def test_insert(self):
        """Test inserting a new record."""
        print("Testing insert operation...")
        self.db.insert("user1", {"name": "Alice", "age": 30})
        result = self.db.get("user1")
        print(f"Inserted user1: {result}")
        self.assertEqual(result, {"name": "Alice", "age": 30})

    def test_update(self):
        """Test updating an existing record."""
        print("Testing update operation...")
        self.db.insert("user2", {"name": "Bob", "age": 25})
        self.db.update("user2", {"name": "Bob", "age": 26})
        result = self.db.get("user2")
        print(f"Updated user2: {result}")
        self.assertEqual(result, {"name": "Bob", "age": 26})

    def test_delete(self):
        """Test deleting a record."""
        print("Testing delete operation...")
        self.db.insert("user3", {"name": "Charlie", "age": 35})
        self.db.delete("user3")
        result = self.db.get("user3")
        print(f"Deleted user3: {result}")
        self.assertIsNone(result)

    def test_display_all(self):
        """Test displaying all records."""
        print("Testing display all operation...")
        self.db.insert("user4", {"name": "David", "age": 40})
        all_data = self.db.display_all()
        print(f"All data: {all_data}")
        self.assertIn("user4", all_data)
        self.assertEqual(all_data["user4"], {"name": "David", "age": 40})

    def test_find_by_field(self):
        """Test finding records by a field value."""
        print("Testing find by field operation...")
        self.db.insert("user5", {"name": "Eve", "age": 22})
        self.db.insert("user6", {"name": "Frank", "age": 22})
        self.db.build_index("age")  # Build index on age
        result = self.db.find_by_field("age", 22)
        print(f"Find by age result: {result}")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], {"user5": {"name": "Eve", "age": 22}})
        self.assertEqual(result[1], {"user6": {"name": "Frank", "age": 22}})

    def test_build_index(self):
        """Test building an index on a field."""
        print("Testing build index operation...")
        self.db.insert("user7", {"name": "Grace", "age": 29})
        self.db.build_index("age")
        result = self.db.find_by_field("age", 29)
        print(f"Build index result: {result}")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {"user7": {"name": "Grace", "age": 29}})

    def test_insert_duplicate_key(self):
        """Test inserting a duplicate key."""
        print("Testing insert duplicate key...")
        self.db.insert("user8", {"name": "Hank", "age": 28})
        with self.assertRaises(KeyError):
            print("Attempting to insert duplicate key 'user8'...")
            self.db.insert("user8", {"name": "Ivy", "age": 27})

    def test_delete_nonexistent_key(self):
        """Test deleting a non-existent key."""
        print("Testing delete non-existent key...")
        with self.assertRaises(KeyError):
            print("Attempting to delete 'nonexistent_key'...")
            self.db.delete("nonexistent_key")

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        print("Tearing down the test database...")
        if os.path.exists("test_database.dm"):
            os.remove("test_database.dm")


if __name__ == '__main__':
    unittest.main()

