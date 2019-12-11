import inspect
import json
import os
import sys


CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
GRANDPARENTDIR = os.path.dirname(PARENTDIR)
sys.path.insert(0, PARENTDIR)
sys.path.insert(0, GRANDPARENTDIR)

from ukrlp_client import UkrlpClient

def get_provider_record():
    ukprn = "10004079"
    records = UkrlpClient.get_matching_provider_records(ukprn)
    print(json.dumps(records, indent=4))
