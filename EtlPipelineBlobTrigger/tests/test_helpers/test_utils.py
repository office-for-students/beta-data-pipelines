"""General purpose functions used in tests"""
import os
from pathlib import Path

def get_string(filename):
    """Reads file into a string and returns"""

    #parent_dir = str(Path().resolve().parent)
    cwd = Path().absolute()
    with open(os.path.join(cwd, filename)) as infile:
        string = infile.read()
    return string
