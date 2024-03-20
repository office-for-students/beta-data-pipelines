import datetime
import logging
import time
from typing import Any
from typing import Dict

import defusedxml.ElementTree as ET
import xmltodict

from constants import COSMOS_COLLECTION_INSTITUTIONS
from constants import COSMOS_DATABASE_ID
from legacy.CreateInst.docs.name_handler import InstitutionProviderNameHandler
from legacy.CreateInst.institution_docs import add_tef_data
from legacy.CreateInst.institution_docs import get_country
from legacy.CreateInst.institution_docs import get_student_unions
from legacy.CreateInst.institution_docs import get_total_number_of_courses
from legacy.CreateInst.institution_docs import get_welsh_uni_names
from legacy.CreateInst.institution_docs import get_white_list
from legacy.CreateInst.locations import Locations
from legacy.services.utils import get_collection_link
from legacy.services.utils import get_cosmos_client
from legacy.services.utils import get_uuid
from legacy.services.utils import normalise_url
from legacy.services.utils import sanitise_address_string


class InstitutionDocs:
    def __init__(self, xml_string: str, version: int) -> None:
        self.version = version
        self.root = ET.fromstring(xml_string)
        self.location_lookup = Locations(self.root)

    def get_institution_element(self, institution: ET) -> Dict[str, Any]:
        """
        Takes an XML element for an institution and builds a dictionary for its data using raw institution data

        :param institution: Institution XML element
        :type institution: ElementTree
        :return: Dictionary of institution data
        :rtype: Dict[str, Any]
        """
        raw_inst_data = xmltodict.parse(ET.tostring(institution))[
            "INSTITUTION"
        ]

        pubukprn = raw_inst_data["PUBUKPRN"]
        institution_element = {}
        if "APROutcome" in raw_inst_data:
            institution_element["apr_outcome"] = raw_inst_data["APROutcome"]

        contact_details = dict(
            address=sanitise_address_string(raw_inst_data.get("PROVADDRESS", "")),
            telephone=raw_inst_data.get("PROVTEL"),
            website=normalise_url(raw_inst_data.get("PROVURL", "")),
        )
        if contact_details:
            institution_element["contact_details"] = contact_details

        institution_element["links"] = {"institution_homepage": contact_details["website"]}

        student_unions = get_student_unions(self.location_lookup, institution)
        if student_unions:
            institution_element["student_unions"] = get_student_unions(self.location_lookup, institution)

        pn_handler = InstitutionProviderNameHandler(
            white_list=get_white_list(),
            welsh_uni_names=get_welsh_uni_names()
        )

        legal_name = raw_inst_data.get("LEGAL_NAME", "")
        first_trading_name = raw_inst_data.get("FIRST_TRADING_NAME", "")
        other_names = raw_inst_data.get("OTHER_NAMES", "")

        institution_element["legal_name"] = legal_name
        if first_trading_name:
            institution_element["first_trading_name"] = first_trading_name
            institution_element["pub_ukprn_name"] = first_trading_name
            institution_element["pub_ukprn_welsh_name"] = pn_handler.get_welsh_uni_name(
                pub_ukprn=pubukprn,
                provider_name=institution_element["first_trading_name"]
            )
        else:
            institution_element["pub_ukprn_name"] = raw_inst_data.get("LEGAL_NAME", "")
            institution_element["pub_ukprn_welsh_name"] = pn_handler.get_welsh_uni_name(
                pub_ukprn=pubukprn,
                provider_name=institution_element["legal_name"]
            )
        if other_names:
            institution_element["other_names"] = other_names.split("###")

        institution_element["pub_ukprn"] = pubukprn
        institution_element["pub_ukprn_country"] = get_country(raw_inst_data["PUBUKPRNCOUNTRY"])
        if "TEFOutcome" in raw_inst_data and not isinstance(raw_inst_data["TEFOutcome"], list):
            institution_element["tef_outcome"] = add_tef_data(raw_inst_data["TEFOutcome"])
        elif isinstance(raw_inst_data.get("TEFOutcome"), list):
            institution_element["tef_outcome"] = list()
            for tef_outcome in raw_inst_data.get("TEFOutcome"):
                institution_element["tef_outcome"].append(add_tef_data(tef_outcome))
        if "QAA_Report_Type" in raw_inst_data or "QAA_URL" in raw_inst_data:
            institution_element["qaa_report_type"] = raw_inst_data.get("QAA_Report_Type")
            institution_element["qaa_url"] = raw_inst_data.get("QAA_URL")
        institution_element["total_number_of_courses"] = get_total_number_of_courses(institution)

        institution_element["ukprn_name"] = institution_element["pub_ukprn_name"]
        institution_element["ukprn_welsh_name"] = institution_element["pub_ukprn_welsh_name"]

        institution_element["ukprn"] = raw_inst_data["UKPRN"]
        institution_element["pub_ukprn_country"] = get_country(raw_inst_data["COUNTRY"])

        return institution_element

    def get_institution_doc(self, institution: ET) -> Dict[str, Any]:
        """
        Takes an institution XML element and converts it to a dictionary.

        :param institution: Institution XML element
        :type institution: ElementTree
        :return: Institution data as a dictionary
        :rtype: Dict[str, Any]
        """
        raw_inst_data = xmltodict.parse(ET.tostring(institution))[
            "INSTITUTION"
        ]

        institution_doc = {
            "_id": get_uuid(),
            "created_at": datetime.datetime.utcnow().isoformat(),
            "version": self.version,
            "institution_id": raw_inst_data["PUBUKPRN"],
            "partition_key": str(self.version),
            "institution": self.get_institution_element(institution),
        }
        return institution_doc

    def create_institution_docs(self) -> None:
        """
        Parse HESA XML and create JSON institution docs in Cosmos DB.

        :return: None
        """

        cosmos_client = get_cosmos_client()
        cosmos_db_client = cosmos_client.get_database_client(COSMOS_DATABASE_ID)
        cosmos_container_client = cosmos_db_client.get_container_client(COSMOS_COLLECTION_INSTITUTIONS)

        collection_link = get_collection_link("COSMOS_COLLECTION_INSTITUTIONS")

        sproc_link = collection_link + "/sprocs/bulkImport"
        partition_key = str(self.version)

        institution_count = 0
        new_docs = []
        sproc_count = 0
        for institution in self.root.iter("INSTITUTION"):
            institution_count += 1
            sproc_count += 1
            new_docs.append(self.get_institution_doc(institution))
            if sproc_count == 100:
                logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
                cosmos_container_client.scripts.execute_stored_procedure(sproc_link, params=[new_docs],
                                                                         partition_key=partition_key)
                logging.info(f"Successfully loaded another {sproc_count} documents")
                # Reset values
                new_docs = []
                sproc_count = 0
                time.sleep(10)

        if sproc_count > 0:
            logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
            cosmos_container_client.scripts.execute_stored_procedure(sproc_link, params=[new_docs],
                                                                     partition_key=partition_key)
            logging.info(f"Successfully loaded another {sproc_count} documents")

        logging.info(f"Processed {institution_count} institutions")
