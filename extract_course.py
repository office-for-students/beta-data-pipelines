import os
import argparse
import sys
import xml.etree.ElementTree as ET


def extract_course(input_file, output_file, pubukprn, kiscourseid):
    """
    Extract a specific KISCOURSE from XML file based on institution and course IDs.
    If course not found in specified institution, searches across all institutions.

    Args:
        input_file (str): Path to input XML file
        output_file (str): Path to output XML file
        pubukprn (str): Institution ID (PUBUKPRN) - can be None to search all institutions
        kiscourseid (str): Course ID (KISCOURSEID)

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check if inputfile exists
        if not os.path.exists(input_file):
            print(f"Error: Input file '{input_file}' not found.")
            return False

        # Parse the XML file
        print(f"Parsing XML file: {input_file}")
        tree = ET.parse(input_file)
        root = tree.getroot()

        course = None
        found_institution = None

        # If pubukprn is specified, try to find the course in that institution first
        if pubukprn:
            # Find the institution with the matching PUBUKPRN
            institution = None
            for inst in root.findall('.//INSTITUTION'):
                if inst.findtext('PUBUKPRN') == pubukprn:
                    institution = inst
                    break

            if institution is None:
                print(f"Warning: Institution with PUBUKPRN '{pubukprn}' not found. Searching all institutions...")
            else:
                # Find the course with the matching KISCOURSEID in the specified institution
                for crs in institution.findall('.//KISCOURSE'):
                    if crs.findtext('KISCOURSEID') == kiscourseid:
                        course = crs
                        found_institution = institution
                        print(f"Found course in specified institution: {pubukprn}")
                        break

        # If course not found in specified institution or no pubukprn specified, search all institutions
        if course is None:
            print(f"Searching for course '{kiscourseid}' across all institutions...")
            for institution in root.findall('.//INSTITUTION'):
                for crs in institution.findall('.//KISCOURSE'):
                    if crs.findtext('KISCOURSEID') == kiscourseid:
                        course = crs
                        found_institution = institution
                        actual_pubukprn = institution.findtext('PUBUKPRN')
                        print(f"Found course in institution: {actual_pubukprn}")
                        break
                if course is not None:
                    break

        if course is None:
            print(f"Error: Course with KISCOURSEID '{kiscourseid}' not found in any institution.")
            return False

        # Create a new XML structure with the course and its institution info
        new_root = ET.Element('KISCOURSE_EXTRACT')

        # Add institution information for context
        if found_institution is not None:
            inst_info = ET.SubElement(new_root, 'INSTITUTION_INFO')
            pubukprn_elem = found_institution.find('PUBUKPRN')
            if pubukprn_elem is not None:
                ET.SubElement(inst_info, 'PUBUKPRN').text = pubukprn_elem.text
            ukprn_elem = found_institution.find('UKPRN')
            if ukprn_elem is not None:
                ET.SubElement(inst_info, 'UKPRN').text = ukprn_elem.text
            inst_name_elem = found_institution.find('INSTNAME')
            if inst_name_elem is not None:
                ET.SubElement(inst_info, 'INSTNAME').text = inst_name_elem.text

        new_root.append(course)

        # Write to output file
        new_tree = ET.ElementTree(new_root)
        new_tree.write(output_file, encoding='utf-8', xml_declaration=True)

        print(f"Successfully extracted course to: {output_file}")
        return True

    except ET.ParseError as e:
        print(f"Error parsing XML file: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


def main():
    """Command-line interface for the XML course extractor"""
    parser = argparse.ArgumentParser(
        description='Extract a specific KISCOURSE from a large XML file. If institution not specified or course not found, searches all institutions.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Search in specific institution first, then all if not found
  python extract_course.py data.xml 10007845 ABC123

  # Search across all institutions (use "None" or omit pubukprn)
  python extract_course.py data.xml None ABC123
  python extract_course.py -i input.xml -c ABC123

  # Using flags
  python extract_course.py -i input.xml -p 10007845 -c ABC123
        '''
    )

    parser.add_argument('input_file', nargs='?', help='Path to the input XML file')
    parser.add_argument('pubukprn', nargs='?', help='Institution ID (PUBUKPRN) - use "None" to search all',
                        default=None)
    parser.add_argument('kiscourseid', nargs='?', help='Course ID (KISCOURSEID)')

    # Alternative using flags
    parser.add_argument('-i', '--input', dest='input_file', help='Path to the input XML file')
    parser.add_argument('-p', '--pubukprn', dest='pubukprn', help='Institution ID (PUBUKPRN)', default=None)
    parser.add_argument('-c', '--courseid', dest='kiscourseid', help='Course ID (KISCOURSEID)')

    args = parser.parse_args()

    # Validate required arguments
    if not args.input_file:
        parser.error("Input file is required")
    if not args.kiscourseid:
        parser.error("Course ID (KISCOURSEID) is required")

    # Handle "None" string for pubukprn
    if args.pubukprn and args.pubukprn.lower() == 'none':
        args.pubukprn = None

    # Generate output filename
    if args.pubukprn:
        output_filename = f"{args.input_file.strip('.xml')}-{args.pubukprn}-{args.kiscourseid}.xml"
    else:
        output_filename = f"{args.input_file.strip('.xml')}-all_institutions-{args.kiscourseid}.xml"

    # Run the extraction
    success = extract_course(
        args.input_file,
        output_filename,
        args.pubukprn,
        args.kiscourseid
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()