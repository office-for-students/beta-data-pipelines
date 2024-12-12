import argparse
import xml.etree.ElementTree as ET


def extract_institution(input_xml, output_xml, pubukprn=None, kiscourse=None):
    try:
        tree = ET.parse(input_xml)
        root = tree.getroot()

        output_tree = ET.ElementTree(ET.Element(root.tag))
        output_root = output_tree.getroot()

        for institution in root.findall('.//INSTITUTION'):
            pubukprn_element = institution.find('PUBUKPRN')
            if pubukprn_element is not None and pubukprn_element.text == pubukprn:
                if kiscourse:
                    course_elements = institution.findall('KISCOURSE')
                    if course_elements is not None:
                        for course in course_elements:
                            kiscourse_element = course.find('KISCOURSEID')
                            if kiscourse_element is not None and kiscourse_element.text == kiscourse:
                                new_inst = ET.Element('INSTITUTION')
                                new_inst.append(course)
                                output_root.append(new_inst)
                else:
                    output_root.append(institution)

        ET.ElementTree(output_root).write(output_xml, encoding="utf-8", xml_declaration=True)
    except ET.ParseError as e:
        print(f"Error parsing input XML: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract specified tags from an XML file.")
    parser.add_argument("input_xml", help="Input XML file name")
    parser.add_argument("output_xml", help="Output XML file name")
    parser.add_argument("--pubukprn", help="PUBKUPRN to search for")
    parser.add_argument("--kiscourse", help="CHIPS_CODE to search for")
    args = parser.parse_args()
    extract_institution(args.input_xml, args.output_xml, args.pubukprn, args.chips_code)
