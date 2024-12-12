import inspect
import os
import sys

CURRENT_DIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
GRANDPARENT_DIR = os.path.dirname(PARENT_DIR)
sys.path.insert(0, PARENT_DIR)
sys.path.insert(0, GRANDPARENT_DIR)
sys.path.append(os.path.join(CURRENT_DIR, "test_helpers"))
