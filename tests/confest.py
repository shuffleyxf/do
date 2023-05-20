import os
import sys

import pytest

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(root_dir, 'src', 'do'))

from api import start


@pytest.fixture(scope="session")
def start_do():
    start(block=False)
