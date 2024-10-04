import xml.etree.ElementTree as ET
import argparse


def create_test_fixtures(input_file, output_file, tag_name, num_tags=1):
    try:
        # Parse the input XML file
        tree = ET.parse(input_file)
        root = tree.getroot()
        new_root = ET.Element(root.tag)

        extracted_count = 0
        for element in root.findall('.//' + tag_name):
            if extracted_count >= num_tags:
                break
            new_root.append(element)
            extracted_count += 1

        with open(output_file, "w"):
            pass

        ET.ElementTree(new_root).write(output_file, encoding="utf-8", xml_declaration=True)
        print(f"Extracted {extracted_count} {tag_name} tags from {input_file} and saved to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract specified tags from an XML file.")
    parser.add_argument("input_file", help="Input XML file name")
    parser.add_argument("output_file", help="Output XML file name")
    parser.add_argument("tag_name", help="Name of the tag to extract")
    parser.add_argument("--num_tags", type=int, default=1, help="Number of tags to extract (default: 1)")

    args = parser.parse_args()

    create_test_fixtures(args.input_file, args.output_file, args.tag_name, args.num_tags)
