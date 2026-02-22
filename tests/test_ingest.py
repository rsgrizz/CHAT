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


class TestIngestCSV(unittest.TestCase):
    def test_iter_csv_rows_emits_rows_with_source_row(self) -> None:
        csv_text = (
            "timestamp,from,to,message,uniqid\n"
            "2026-02-21 10:00:00,Alice,Bob,Hello,1\n"
            "2026-02-21 10:01:00,Bob,Alice,Hi,2\n"
        )

        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, "msgs.csv")
            with open(p, "w", encoding="utf-8", newline="") as f:
                f.write(csv_text)

            rows = list(iter_csv_rows(p))

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0].source_row, 1)
            self.assertEqual(rows[1].source_row, 2)
            self.assertEqual(rows[0].data["message"], "Hello")
            self.assertEqual(rows[1].data["uniqid"], "2")


if __name__ == "__main__":
    unittest.main()