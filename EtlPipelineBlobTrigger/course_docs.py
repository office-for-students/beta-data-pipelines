import datetime
import json
import pprint
import uuid
import xmltodict
import xml.etree.ElementTree as ET


def get_uuid():
    id = uuid.uuid1()
    return(id.hex)


def build_course_section(inst_dict, course_dict):
    country_code_lookup = {
        'XF': 'England',
        'XG': 'Northern Ireland',
        'XH': 'Scotland',
        'XI': 'Wales'
    }

    d = {}
    if 'UKPRNAPPLY' in course_dict:
        d['application_provider'] = course_dict['UKPRNAPPLY']

    country_code = inst_dict['INSTITUTION']['COUNTRY']
    country_name = country_code_lookup[country_code]
    d['country'] = {'code': country_code, 'name': country_name}
    return d


def build_json_dict(inst_dict, course_dict):
    json_dict = {}
    json_dict['id'] = get_uuid()
    json_dict['created_at'] = datetime.datetime.utcnow().isoformat()
    json_dict['version'] = 1

    json_dict['course'] = build_course_section(inst_dict, course_dict)
    return json_dict


fname = "../test-data/test-3-inst.xml"
tree = ET.parse(fname)
root = tree.getroot()

count = 0
for institution in root.iter('INSTITUTION'):
    inst_dict = xmltodict.parse(ET.tostring(institution))
    for course in institution.findall('KISCOURSE'):
        course_dict = xmltodict.parse(ET.tostring(course))['KISCOURSE']
        json_dict = build_json_dict(inst_dict, course_dict)
        print(json.dumps(json_dict, indent=4))
        if count > 1:
            break
    if count > 1:
        break
    count += 1