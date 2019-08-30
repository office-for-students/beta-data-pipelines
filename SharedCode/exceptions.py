#!/usr/bin/env python

class Error(Exception):
    """ Base Exception Class """

    pass


class StopEtlPipelineWarningException(Error):
    """ A warning is raised during the ETL Pipeline """

    pass


class StopEtlPipelineErrorException(Error):
    """ An error is raised during the ETL Pipeline """

    pass


class XmlValidationError(Error):
    """ An error is raised during XML Validation """

    pass

class DataSetTooEarlyError(Error):
    """ An error raised if too soon for DataSet service run again """

    pass
