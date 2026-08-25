import csv
from pprint import pprint

import defusedxml.ElementTree as ET
import xmltodict

from EtlPipeline.accreditations import Accreditations
from EtlPipeline.course_docs import get_course_doc
from EtlPipeline.course_docs import get_locids
from EtlPipeline.kisaims import KisAims
from EtlPipeline.locations import Locations
from EtlPipeline.sector_salaries import GOSectorSalaries
from EtlPipeline.sector_salaries import LEO3SectorSalaries
from EtlPipeline.sector_salaries import LEO5SectorSalaries
from EtlPipeline.subject_enricher import SubjectCourseEnricherBase


class LocalSubjectCourseEnricher(SubjectCourseEnricherBase):
    def __init__(self, version):
        self.version = version
        super().__init__(version)
        self.subject_lookups = get_local_subject_data("docs/english-and-wesh-subject-codes.csv")


def get_local_subject_data(filename):
    with open(filename, newline="", encoding="utf-8-sig") as f:
        return {
            row["code"]: {
                "english_name": row["english_label"],
                "level": int(row["level"]),
                "welsh_name": row["welsh_label"],
            }
            for row in csv.DictReader(f)
        }


if __name__ == "__main__":
    with open("course_broken.xml", "r") as xml_file:
        xml_string = xml_file.read()

    root = ET.fromstring(xml_string)

    accreditations = Accreditations(root)
    kisaims = KisAims(root)
    locations = Locations(root)

    go_sector_salaries = GOSectorSalaries(root)
    leo3_sector_salaries = LEO3SectorSalaries(root)
    leo5_sector_salaries = LEO5SectorSalaries(root)

    ver = 0
    local_subject_enricher = LocalSubjectCourseEnricher(ver)

    for institution in root.iter("INSTITUTION"):

        raw_inst_data = xmltodict.parse(ET.tostring(institution))[
            "INSTITUTION"
        ]
        ukprn = raw_inst_data["UKPRN"]

        for course in institution.findall("KISCOURSE"):
            raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
            raw_course_data['KISMODE'] = str(int(raw_course_data['KISMODE']))
            raw_course_data['KISLEVEL'] = str(int(raw_course_data['KISLEVEL']))

            locids = get_locids(raw_course_data, ukprn)
            course_doc = get_course_doc(
                accreditations,
                locations,
                locids,
                raw_inst_data,
                raw_course_data,
                kisaims,
                ver,
                go_sector_salaries,
                leo3_sector_salaries,
                leo5_sector_salaries,
                local_subject_enricher
            )
            local_subject_enricher.enrich_course(course_doc)
            pprint(course_doc)

            # with open("course_broken.json", "w") as f:
            #     f.write(json.dumps(course_doc, indent=4))
            # course_stats = get_stats(
            #     raw_course_data,
            # )
            # pprint(course_stats)
