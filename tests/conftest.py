"""
Pytest configuration and shared fixtures for here-i-strand (HIS) tests.

Ensures the project root is on sys.path so imports like `from his.his import ...`
work regardless of where pytest is invoked from.
"""

import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# pytest-asyncio uses asyncio_mode = auto when the plugin is installed.
