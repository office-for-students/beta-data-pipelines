from typing import Any
from typing import Dict
from typing import List

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class JobList:
    """Extracts and transforms the COMMON entries in a KISCOURSE"""

    """
    The following is the agreed structure that COMMON elements in a KISCOURSE
    element will be transformed to:

    "job_list": [{
        "aggregation_level": "integer",
        "list": [{
            "job": "string",
            "percentage_of_students": "integer",
            "order": "integer",
        }],
        "number_of_students": "integer",
        "response_rate": "integer",
        "subject": {
            "code": "string",
            "english_label": "string",
            "welsh_label": "string"
        },
        "unavailable": {
            "code": "integer",
            "reason": "string"
        }
    }]

    """

    def __init__(self) -> None:
        self.xml_element_key = "COMMON"
        self.xml_subj_key = "COMSBJ"
        self.xml_agg_key = "COMAGG"
        self.xml_unavail_reason_key = "COMUNAVAILREASON"
        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )
        self.data_fields_lookup = self.shared_utils.get_lookup(
            "common_data_fields"
        )

    def get_stats(self, raw_course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extracts and transforms the COMMON entries in a KISCOURSE"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(
            raw_course_data, self.xml_element_key
        )
        for xml_elem in raw_xml_list:
            json_elem = {}
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(
                    xml_elem
                )
            json_elem_list.append(json_elem)
        return json_elem_list

    def get_json_data(self, xml_elem: Dict[str, Any]) -> Dict[str, Any]:
        """Extracts and transforms a COMMON entry with data in a KISCOURSE"""

        lookup = self.data_fields_lookup
        json_data = {}
        for xml_key in lookup:
            if lookup[xml_key][1] == "M":
                json_data[
                    lookup[xml_key][0]
                ] = self.shared_utils.get_json_value(xml_elem.get(xml_key))
            else:
                if xml_key in xml_elem:
                    json_key = lookup.get(xml_key, [])[0]
                    if json_key == "subject":
                        json_data[json_key] = self.shared_utils.get_subject(
                            xml_elem
                        )
                    elif json_key == "list":
                        json_data["list"] = self.get_list_field(xml_elem)
                    else:
                        json_data[json_key] = self.shared_utils.get_json_value(
                            xml_elem.get(xml_key)
                        )
        return json_data

    def get_list_field(self, xml_elem: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extracts and transforms the JOBLIST entries in a COMMON element"""

        list_field = []
        job_lists = self.shared_utils.get_raw_list(xml_elem, "JOBLIST")
        for job_list in job_lists:
            job_list_item = {}
            job_list_item["job"] = job_list.get("JOB")
            job_list_item["percentage_of_students"] = job_list.get("PERC")

            if job_list_item.get("percentage_of_students").isnumeric():
                if int(job_list_item.get("percentage_of_students")) < 5:
                    job_list_item["percentage_of_students"] = "<5"

            job_list_item["order"] = job_list.get("ORDER")
            job_list_item["hs"] = job_list.get("HS")
            list_field.append(job_list_item)
        return list_field
