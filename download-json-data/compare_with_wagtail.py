#!/usr/bin/env python
import json

with open('institutions.json') as file:
    beta_data_pipelines_institutions = json.load(file)

with open('../../wagtail-CMS/CMS/static/jsonfiles/institutions_en.json') as file:
    wagtail_institutions = json.load(file)

lines = []

for bdp_i in beta_data_pipelines_institutions:
    institute_name = bdp_i['institution']['pub_ukprn_name']
    institute_found = False
    for wg_i in wagtail_institutions:
        if institute_name == wg_i['name']:
            institute_found = True
    if institute_found:
        lines.append('Success: ' + institute_name + ' was found in the institutions_en.json')
    else:
        lines.append('ERROR: ' + institute_name + ' was NOT found in the institutions_en.json')

with open('results_wagtail.txt', 'w') as f:
     f.writelines("%s\n" % i for i in lines)
     f.close()
