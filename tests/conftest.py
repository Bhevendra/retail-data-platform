
import sys
from pathlib import Path

# Find the project root: parent of the tests directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Make common_utils importable during pytest collection.
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
