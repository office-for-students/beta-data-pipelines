"""General purpose functions used in tests"""
import os
from pathlib import Path

file_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(file_dir)

def get_string(filename):
    """Reads file into a string and returns"""

    #parent_dir = str(Path().resolve().parent)
    cwd = Path().absolute()
    with open(os.path.join(parent_dir, filename)) as infile:
        string = infile.read()
    return string
