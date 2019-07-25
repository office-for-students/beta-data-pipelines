import os
import sys
import inspect

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.insert(0, CURRENTDIR)

def get_eng_welsh_item(key, lookup_table):
    item = {}
    keyw = key + 'W'
    if key in lookup_table:
        item['english'] = lookup_table[key]
    if keyw in lookup_table:
        item['welsh'] = lookup_table[keyw]
    return item
