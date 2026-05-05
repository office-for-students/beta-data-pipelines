import csv
import os
from lxml import etree


def extract_hesa_data_to_csv_iterative(xml_file_path, csv_file_path=None):
    """
    Iterative parsing version for very large files, extracting PUBUKPRN, KISCOURSEID, and KISMODE.
    """
    if csv_file_path is None:
        base_name = os.path.splitext(xml_file_path)[0]
        csv_file_path = f"{base_name}.csv"

    try:
        context = etree.iterparse(xml_file_path, events=('end',), tag='KISCOURSE')

        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['PUBUKPRN', 'KISCOURSEID', 'KISMODE'])

            record_count = 0

            for event, elem in context:
                # Extract KISCOURSEID
                kiscourseid_elem = elem.find('KISCOURSEID')
                kiscourseid = kiscourseid_elem.text if kiscourseid_elem is not None and kiscourseid_elem.text else ''

                # Extract KISMODE
                kismode_elem = elem.find('KISMODE')
                kismode = kismode_elem.text if kismode_elem is not None and kismode_elem.text else ''

                # Extract PUBUKPRN
                pubukprn = None
                pubukprn_elem = elem.find('PUBUKPRN')
                if pubukprn_elem is not None and pubukprn_elem.text:
                    pubukprn = pubukprn_elem.text

                # If PUBUKPRN not found in KISCOURSE, check parent
                if pubukprn is None:
                    parent = elem.getparent()
                    if parent is not None:
                        pubukprn_elem = parent.find('PUBUKPRN')
                        if pubukprn_elem is not None and pubukprn_elem.text:
                            pubukprn = pubukprn_elem.text

                # If we have at least some data, write to CSV
                if pubukprn or kiscourseid or kismode:
                    writer.writerow([pubukprn or '', kiscourseid, kismode])
                    record_count += 1

                # Clean up memory
                elem.clear()
                parent = elem.getparent()
                if parent is not None:
                    parent.remove(elem)

            del context

        print(f"Successfully extracted {record_count} records to {csv_file_path}")
        return record_count

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        return -1


def main():
    extract_hesa_data_to_csv_iterative("kis20250902120928.xml", "output.csv")


if __name__ == "__main__":
    main()