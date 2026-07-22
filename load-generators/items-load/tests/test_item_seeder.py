import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import item_seeder


class FakeCursor:
    def __init__(self, existing_count):
        self.existing_count = existing_count
        self.closed = False
        self.executions = []

    def execute(self, query, params=None):
        self.executions.append((query, params))

    def fetchone(self):
        return (self.existing_count,)

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, existing_count=0):
        self.cursor_instance = FakeCursor(existing_count)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class SeedSettingsTests(unittest.TestCase):
    def test_defaults_are_finite_and_idempotent(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(item_seeder.get_seed_settings(), (1000, "skip-if-present"))

    def test_invalid_count_fails(self):
        with patch.dict(os.environ, {"ITEM_SEED_COUNT": "0"}, clear=True):
            with self.assertRaisesRegex(ValueError, "greater than zero"):
                item_seeder.get_seed_settings()

    def test_invalid_mode_fails(self):
        with patch.dict(os.environ, {"ITEM_SEED_MODE": "replace"}, clear=True):
            with self.assertRaisesRegex(ValueError, "ITEM_SEED_MODE"):
                item_seeder.get_seed_settings()


class SeedItemsTests(unittest.TestCase):
    @patch("item_seeder.execute_values")
    @patch("item_seeder.generate_item", return_value=("Item", "widgets", 10.0, 100))
    def test_empty_table_is_seeded(self, _generate_item, execute_values):
        connection = FakeConnection(existing_count=0)

        inserted = item_seeder.seed_items(connection, 3, "skip-if-present")

        self.assertEqual(inserted, 3)
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)
        execute_values.assert_called_once()
        self.assertTrue(connection.cursor_instance.closed)

    @patch("item_seeder.execute_values")
    def test_rerun_skips_existing_table(self, execute_values):
        connection = FakeConnection(existing_count=3)

        inserted = item_seeder.seed_items(connection, 3, "skip-if-present")

        self.assertEqual(inserted, 0)
        self.assertEqual(connection.commits, 1)
        execute_values.assert_not_called()

    @patch("item_seeder.execute_values")
    @patch("item_seeder.generate_item", return_value=("Item", "widgets", 10.0, 100))
    def test_append_mode_is_explicit(self, _generate_item, execute_values):
        connection = FakeConnection(existing_count=3)

        inserted = item_seeder.seed_items(connection, 2, "append")

        self.assertEqual(inserted, 2)
        execute_values.assert_called_once()


if __name__ == "__main__":
    unittest.main()
