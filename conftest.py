"""Shared test configuration.

* Puts the repository root on ``sys.path`` so ``storage``, ``serving``,
  ``batch_layer``, ``streaming_layer`` and ``data_generator`` import without
  being installed.
* Redirects SQLite to a throw-away file so unit tests never write into the
  committed ``storage/fleet_db.sqlite`` or into ``data_lake/``.
"""

import os
import sys
import tempfile

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

os.environ.setdefault("DB_ENGINE", "sqlite")
os.environ.setdefault(
    "SQLITE_DB_PATH",
    os.path.join(tempfile.mkdtemp(prefix="fleet-tests-"), "test_fleet.sqlite"),
)
