#!/usr/bin/env python
import logging
import traceback
from datetime import datetime
from typing import Any

from constants import BLOB_HESA_BLOB_NAME
from constants import BLOB_HESA_CONTAINER_NAME
from constants import BLOB_WELSH_UNIS_BLOB_NAME
from constants import BLOB_WELSH_UNIS_CONTAINER_NAME
from institution_creation.docs.institution_docs import InstitutionDocs
from institution_creation.docs.name_handler import InstitutionProviderNameHandler
from institution_creation.institution_docs import get_welsh_uni_names
from institution_creation.institution_docs import get_white_list
from services import exceptions


# from SharedCode.mail_helper import MailHelper


def create_institutions_main(
        blob_service: type['BlobServiceBase'],
        cosmos_service: type['CosmosServiceBase'],
        dataset_service: type['DataSetServiceBase']
) -> dict[str, Any]:
    response = {}

    try:
        logging.info(
            f"CreateInst message queue triggered\n"
        )

        function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"CreateInst function started on {function_start_datetime}"
        )

        """ DECOMPRESSION - Decompress the compressed HESA XML """
        # The XML blob provided to this function will be gzip compressed.
        # This is a work around for a limitation discovered in Azure,
        # where Functions written in Python do not get triggered # correctly with large blobs. Tests showed this is not a limitation
        # with Funtions written in C#.

        hesa_xml_file_as_string = blob_service.get_str_file(
            container_name=BLOB_HESA_CONTAINER_NAME,
            blob_name=BLOB_HESA_BLOB_NAME
        )

        version = dataset_service.get_latest_version_number()

        """ LOADING - extract data and load JSON Documents """

        logging.info(f"using version number: {version}")
        dataset_service.update_status("institutions", "in progress")

        csv_string = blob_service.get_str_file(
            container_name=BLOB_WELSH_UNIS_CONTAINER_NAME,
            blob_name=BLOB_WELSH_UNIS_BLOB_NAME
        )
        provider_name_handler = InstitutionProviderNameHandler(
            white_list=get_white_list(),
            welsh_uni_names=get_welsh_uni_names(csv_string)
        )

        inst_docs = InstitutionDocs(
            xml_string=hesa_xml_file_as_string,
            version=version,
            provider_name_handler=provider_name_handler,
            cosmos_service=cosmos_service
        )
        # cosmos_service = get_cosmos_service(COSMOS_COLLECTION_INSTITUTIONS)

        inst_docs.create_institution_docs()
        dataset_service.update_status("institutions", "succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        message = f"CreateInst successfully finished on {function_end_datetime}"

        logging.info(message)
        response["message"] = message
        response["statusCode"] = 200

        # msgout.set(msgin.get_body().decode("utf-8") + msgerror)

    except exceptions.StopEtlPipelineWarningException:

        # A WARNING is raised while the function is running and
        # StopEtlPipelineOnWarning=True. For example, the incoming raw XML
        # is not valid against its XSD
        error_message = (
            "A WARNING has been encountered while the function is running. "
            "The function will be stopped since StopEtlPipelineOnWarning is "
            "set to TRUE in the Application Settings."
        )
        logging.error(error_message)

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(
        #     f"Automated data import failed on {function_fail_datetime} at institution_creation" + msgin.get_body().decode("utf-8") + msgerror,
        #     f"Data Import {environment} - {function_fail_date} - Failed"
        # )

        message = f"CreateInst failed on {function_fail_datetime}"

        logging.error(message)
        dataset_service.update_status("institutions", "failed")

        response["message"] = message + f".\n{error_message}"
        response["statusCode"] = 500
        # raise Exception(error_message)

    except Exception as e:
        # Unexpected exception
        dataset_service.update_status("institutions", "failed")

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(
        #     f"Automated data import failed on {function_fail_datetime} at institution_creation" + msgin.get_body().decode("utf-8") + msgerror,
        #     f"Data Import {environment} - {function_fail_date} - Failed"
        # )

        message = f"CreateInst failed on {function_fail_datetime}"

        logging.error(message, exc_info=True)

        response["message"] = message
        response["exception"] = traceback.format_exc()
        response["statusCode"] = 500

        # Raise to Azure
        # raise e

    return response