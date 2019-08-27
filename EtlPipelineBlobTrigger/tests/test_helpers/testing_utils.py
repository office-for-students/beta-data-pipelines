"""General purpose functions used in tests"""
import os

FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(FILE_DIR)


def get_string(filename):
    """Reads file into a string and returns"""
    with open(os.path.join(PARENT_DIR, filename)) as infile:
        string = infile.read()
    return string

def get_first(Node):
    for x in Node:
        return x

def remove_variable_elements(doc):
    keys_to_delete = ['_id', 'created_at']
    for key in keys_to_delete:
        del doc[key]
    return doc
