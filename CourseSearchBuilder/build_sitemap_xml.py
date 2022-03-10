from datetime import datetime

base_url = "https://discoveruni.gov.uk"


def build_course_details_url(institution_id: str, course_id: str, kis_mode: str, language: str) -> str:
    return f"{base_url}/{language}/course-details/{institution_id}/{course_id}/{kis_mode}/"


def build_institution_details_url(institution_id: str, language: str) -> str:
    return f"{base_url}/{language}/institution-details/{institution_id}/"


def build_xml_string(arg_list: tuple, xml: str) -> str:
    today = datetime.strftime(datetime.today(), "%Y-%m-%d")
    centre_xml = """"""
    for args in arg_list:
        if len(args) == 4:
            centre_xml += f"""    <url>
                    <loc>{build_course_details_url(*args)}></loc>
                    <lastmod>{today}</lastmod>
                </url>
            """
        elif len(args) == 2:
            centre_xml += f"""    <url>
                    <loc>{build_institution_details_url(*args)}></loc>
                    <lastmod>{today}</lastmod>
                </url>
            """
    final_xml = f"""{xml}
            {centre_xml}</urlset>"""
    return final_xml
