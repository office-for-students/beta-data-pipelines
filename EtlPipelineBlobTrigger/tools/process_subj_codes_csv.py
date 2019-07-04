"""
Create json lookup files for subject code to english and subject code
to welsh.
"""

import csv
import json


with open('subj_codes.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    subj_code_english = {}
    subj_code_welsh = {}
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        else:
            subj_code_english[row[0]] = row[1]
            line_count += 1

    with open('subj_code_english.json', 'w') as fp:
        json.dump(subj_code_english, fp, indent=4)

    print(f'Processed {line_count} lines.')
