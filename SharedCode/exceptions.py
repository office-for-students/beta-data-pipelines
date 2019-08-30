#!/usr/bin/env python

""" exceptions.py: ETL Pipeline Custom Exceptions """

__author__ = "Jillur Quddus, Nathan Shumoogum"
__credits__ = ["Jillur Quddus", "Nathan Shumoogum"]
__version__ = "0.1"
_maintainer__ = "Jillur Quddus"
__email__ = "jillur.quddus@methods.co.uk"
__status__ = "Development"


class Error(Exception):
   """ Base Exception Class """
   pass

class StopEtlPipelineWarningException(Error):
    """ A warning is raised during the ETL Pipeline """
    pass

class StopEtlPipelineErrorException(Error):
    """ An error is raised during the ETL Pipeline """
    pass
