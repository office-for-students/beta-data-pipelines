import xml.etree.ElementTree as ET
import argparse
import sys
import os


def extract_course(input_file, output_file, pubukprn, kiscourseid):
    """
    Extract a specific KISCOURSE from XML file based on institution and course IDs

    Args:
        input_file (str): Path to input XML file
        output_file (str): Path to output XML file
        pubukprn (str): Institution ID (PUBUKPRN)
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

        # Find the institution with the matching PUBUKPRN
        institution = None
        for inst in root.findall('.//INSTITUTION'):
            if inst.findtext('PUBUKPRN') == pubukprn:
                institution = inst
                break

        if institution is None:
            print(f"Error: Institution with PUBUKPRN '{pubukprn}' not found.")
            return False

        # Find the course with the matching KISCOURSEID
        course = None
        for crs in institution.findall('.//KISCOURSE'):
            if crs.findtext('KISCOURSEID') == kiscourseid:
                course = crs
                break

        if course is None:
            print(f"Error: Course with KISCOURSEID '{kiscourseid}' not found in institution '{pubukprn}'.")
            return False

        # Create a new XML structure with just the course
        new_root = ET.Element('KISCOURSE_EXTRACT')
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
        description='Extract a specific KISCOURSE from a large XML file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python extract_course.py data.xml 10007845 ABC123
  python extract_course.py -i input.xml -p 10007845 -c ABC123
        '''
    )

    parser.add_argument('input_file', help='Path to the input XML file')
    parser.add_argument('pubukprn', help='Institution ID (PUBUKPRN)')
    parser.add_argument('kiscourseid', help='Course ID (KISCOURSEID)')

    # Alternative using flags
    parser.add_argument('-i', '--input', dest='input_file', help='Path to the input XML file')
    parser.add_argument('-p', '--pubukprn', dest='pubukprn', help='Institution ID (PUBUKPRN)')
    parser.add_argument('-c', '--courseid', dest='kiscourseid', help='Course ID (KISCOURSEID)')

    args = parser.parse_args()

    # Run the extraction
    success = extract_course(
        args.input_file,
        f"{args.input_file.strip('.xml')}-{args.pubukprn}-{args.kiscourseid}.xml",
        args.pubukprn,
        args.kiscourseid
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()