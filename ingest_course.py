import argparse
import json
import sys

import defusedxml.ElementTree as ET
import xmltodict
from dotenv import load_dotenv

from EtlPipeline.accreditations import Accreditations
from EtlPipeline.course_docs import get_course_doc
from EtlPipeline.course_docs import get_locids
from EtlPipeline.kisaims import KisAims
from EtlPipeline.locations import Locations
from EtlPipeline.qualification_enricher import QualificationCourseEnricher
from EtlPipeline.sector_salaries import GOSectorSalaries
from EtlPipeline.sector_salaries import LEO3SectorSalaries
from EtlPipeline.sector_salaries import LEO5SectorSalaries
from EtlPipeline.subject_enricher import SubjectCourseEnricher
from EtlPipeline.ukrlp_enricher import UkRlpCourseEnricher


def main():
    """Command-line interface for the XML course ingester"""
    load_dotenv()

    parser = argparse.ArgumentParser(
        description='Ingests a specific KISCOURSE from a large XML file. Both institution ID and course ID must be provided.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Ingest a course from a large XML file using course and institution ID
  python extract_course.py data.xml 10007845 ABC123

  # Using flags
  python extract_course.py -i input.xml -p 10007845 -c ABC123


Requires the following environment variables to be set in either a .env file or os.env to function correctly:
- AzureCosmosDbDatabaseId
- AzureCosmosDbUri
- AzureCosmosDbKey
- AzureCosmosDbInstitutionsCollectionId
- AzureCosmosDbSubjectsCollectionId
- AzureStorageAccountConnectionString
- AzureStorageQualificationsContainerName
- AzureStorageQualificationsBlobName
- AzureStorageSubjectsContainerName
- AzureStorageSubjectsBlobName
        '''
    )

    parser.add_argument('input_file', nargs='?', help='Path to the input XML file')
    parser.add_argument('pubukprn', nargs='?', help='Institution ID (PUBUKPRN)')
    parser.add_argument('kiscourseid', nargs='?', help='Course ID (KISCOURSEID)')

    # Alternative using flags
    parser.add_argument('-i', '--input', dest='input_file', help='Path to the input XML file')
    parser.add_argument('-p', '--pubukprn', dest='pubukprn', help='Institution ID (PUBUKPRN)')
    parser.add_argument('-c', '--courseid', dest='kiscourseid', help='Course ID (KISCOURSEID)')

    args = parser.parse_args()

    # Validate required arguments
    if not args.input_file:
        parser.error("Input file is required")
    if not args.pubukprn:
        parser.error("Institution ID (PUBUKPRN) is required")

    # Handle "None" string for kiscourseid
    if args.kiscourseid and args.kiscourseid.lower() == 'none':
        args.kiscourseid = None

    # Generate output filename
    if args.kiscourseid is not None:
        output_filename = f"ingest-{args.pubukprn}-{args.kiscourseid}.json"
    else:
        output_filename = f"ingest-{args.pubukprn}.json"

    # Run the ingestion for the specified course
    success = ingest_course(
        args.input_file,
        output_filename,
        args.pubukprn,
        args.kiscourseid
    )

    sys.exit(0 if success else 1)


def ingest_course(
        input_xml_filename,
        output_filename,
        pubukprn,
        kiscourseid = None
):
    """
    Ingests courses from a large XML file based on institution and course IDs.

    :param input_xml_filename: Path to the input XML file
    :param output_filename: Path to output file to be written
    :param pubukprn: Institution ID (PUBUKPRN)
    :param kiscourseid: Course ID (KISCOURSEID), if not provided will ingest all courses for the institution
    """
    try:
        print(f"Loading XML file {input_xml_filename}")
        with open(input_xml_filename, "r") as xml_file:
            xml_string = xml_file.read()

        root = ET.fromstring(xml_string)

        accreditations = Accreditations(root)
        kisaims = KisAims(root)
        locations = Locations(root)

        go_sector_salaries = GOSectorSalaries(root)
        leo3_sector_salaries = LEO3SectorSalaries(root)
        leo5_sector_salaries = LEO5SectorSalaries(root)

        version = 439
        enricher = UkRlpCourseEnricher(version)
        subject_enricher = SubjectCourseEnricher(version)
        qualification_enricher = QualificationCourseEnricher()

        output_data = []

        institution = root.find(f".//INSTITUTION[PUBUKPRN='{pubukprn}']")
        if institution is None:
            print(f"Error: Institution with PUBUKPRN '{pubukprn}' not found.")
            return False

        raw_inst_data = xmltodict.parse(ET.tostring(institution))[
            "INSTITUTION"
        ]
        ukprn = raw_inst_data["UKPRN"]

        if kiscourseid is None:
            courses_to_ingest = institution.findall("KISCOURSE")
        else:
            courses_to_ingest = [institution.find(f".//KISCOURSE[KISCOURSEID='{kiscourseid}']")]

        for course in courses_to_ingest:
            raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
            raw_course_data['KISMODE'] = str(int(raw_course_data['KISMODE']))
            raw_course_data['KISLEVEL'] = str(int(raw_course_data['KISLEVEL']))

            locids = get_locids(raw_course_data, ukprn)
            print(f"Ingesting course: {raw_course_data['KISCOURSEID']}")
            course_doc = get_course_doc(
                accreditations,
                locations,
                locids,
                raw_inst_data,
                raw_course_data,
                kisaims,
                version,
                go_sector_salaries,
                leo3_sector_salaries,
                leo5_sector_salaries,
                subject_enricher
            )
            enricher.enrich_course(course_doc)
            subject_enricher.enrich_course(course_doc)
            qualification_enricher.enrich_course(course_doc)

            output_data.append(course_doc)

        with open(output_filename, "w") as f:
            f.write(json.dumps(output_data, indent=4))

        return True

    except Exception as e:
        print(f"Error: {e}")
        return False


if __name__ == "__main__":
    main()
