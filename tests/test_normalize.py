"""
Created by: Randy Grizzelli
Email: grizzellir@gmail.com
GitHub: https://github.com/rsgrizz
Version: 0.1
Date: 2026-02-21
Purpose: Communication Heuristics Analysis for Triage (CHAT) engine for deterministic prioritization of exported communications.
"""

import os
import tempfile
import unittest

from chat_engine.core.ingest import iter_csv_rows
from chat_engine.core.normalize import SchemaMapping, normalize_row


class TestNormalize(unittest.TestCase):
    def test_normalize_row_generates_msg_id_when_missing(self) -> None:
        csv_text = (
            "timestamp,from,to,message\n"
            "2026-02-21 10:00:00,Alice,Bob,Hello\n"
        )

        mapping = SchemaMapping(
            timestamp_col="timestamp",
            from_col="from",
            to_col="to",
            message_col="message",
            uniqid_col=None,
        )

        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, "msgs.csv")
            with open(p, "w", encoding="utf-8", newline="") as f:
                f.write(csv_text)

            row = list(iter_csv_rows(p))[0]
            nm = normalize_row(row, mapping)

            self.assertTrue(nm.msg_id.startswith("ROW1:"))
            self.assertEqual(nm.sender, "Alice")
            self.assertEqual(nm.recipient, "Bob")
            self.assertEqual(nm.body, "Hello")
            self.assertEqual(nm.source_row, 1)

    def test_timestamp_parses_to_utc(self) -> None:
        csv_text = (
            "timestamp,from,to,message,uniqid\n"
            "2026-02-21 10:00:00,Alice,Bob,Hello,abc\n"
        )

        mapping = SchemaMapping(
            timestamp_col="timestamp",
            from_col="from",
            to_col="to",
            message_col="message",
            uniqid_col="uniqid",
        )

        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, "msgs.csv")
            with open(p, "w", encoding="utf-8", newline="") as f:
                f.write(csv_text)

            row = list(iter_csv_rows(p))[0]
            nm = normalize_row(row, mapping, assume_tz="America/New_York")

            self.assertEqual(nm.msg_id, "abc")
            self.assertIsNotNone(nm.ts_utc)
            self.assertTrue(nm.ts_utc.endswith("Z"))


if __name__ == "__main__":
    unittest.main()