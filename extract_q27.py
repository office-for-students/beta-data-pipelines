import xml.etree.ElementTree as ET
import argparse
import sys
import os


def extract_course(course_file):
    """
    Extract a specific KISCOURSE from XML file based on an institution and course IDs

    Args:
        course_file (str): Path to input XML file
        output_file (str): Path to output XML file
        pubukprn (str): Institution ID (PUBUKPRN)
        kiscourseid (str): Course ID (KISCOURSEID)

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check if an input file exists
        if not os.path.exists(course_file):
            print(f"Error: Input file '{course_file}' not found.")
            return False

        # Parse the XML file
        print(f"Parsing XML file: {course_file}")
        tree = ET.parse(course_file)
        root = tree.getroot()


        for value in root.findall('.//Q27'):
           print(value.text)

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
        description='Extract a specific Q27 from a large XML file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python extract_course.py  output_course.xml 10007845 ABC123
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
        f"{args.input_file.strip('.xml')}-{args.pubukprn}-{args.kiscourseid}-q27.xml",
        args.pubukprn,
        args.kiscourseid
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()