# tests/conftest.py
import sys
from pathlib import Path

# Add the project root (parent of tests/) to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))
