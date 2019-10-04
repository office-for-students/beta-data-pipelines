import inspect
import os
import sys

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
GRANDPARENTDIR = os.path.dirname(PARENTDIR)
sys.path.insert(0, PARENTDIR)
sys.path.insert(0, GRANDPARENTDIR)
