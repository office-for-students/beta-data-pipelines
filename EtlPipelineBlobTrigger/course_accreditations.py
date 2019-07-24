"""Data transformation code for accreditation data."""

import course_lookup_tables as lookup
from course_stats import SharedUtils


def get_accreditations(raw_course_data, acc_lookup):
    acc_list = []
    raw_xml_list = SharedUtils.get_raw_list(raw_course_data,
                                            'ACCREDITATION')

    for xml_elem in raw_xml_list:
        json_elem = {}

        if 'ACCTYPE' in xml_elem:
            json_elem['type'] = xml_elem['ACCTYPE']
            accreditations = acc_lookup.get_accreditation_data_for_key(
                                xml_elem['ACCTYPE'])

            if 'ACCURL' in accreditations:
                json_elem['accreditor_url'] = accreditations['ACCURL']

            text = {}
            if 'ACCTEXT' in accreditations:
                text['english'] = accreditations['ACCTEXT']

            if 'ACCTEXTW' in accreditations:
                text['welsh'] = accreditations['ACCTEXTW']

            json_elem['text'] = text

        if 'ACCDEPENDURL' in xml_elem or 'ACCDEPENDURLW' in xml_elem:

            urls = {}
            if 'ACCDEPENDURL' in xml_elem:
                urls['english'] = xml_elem['ACCDEPENDURL']

            if 'ACCDEPENDURLW' in xml_elem:
                urls['welsh'] = xml_elem['ACCDEPENDURLW']

            json_elem['url'] = urls

        if 'ACCDEPEND' in xml_elem:
            dependent_on = {}
            dependent_on['code'] = xml_elem['ACCDEPEND']
            dependent_on['label'] = lookup.accreditation_code[
                                        xml_elem['ACCDEPEND']]

            json_elem['dependent_on'] = dependent_on

        acc_list.append(json_elem)

    return acc_list
