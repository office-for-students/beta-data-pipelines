import logging
import requests

from SharedCode import exceptions
from . import models

def delete_index(url, headers, index_name):
	try:
		response = requests.delete(url, headers=headers)
	except requests.exceptions.RequestException as e:
		logging.exception('unexpected error deleting index',exc_info=True)
		raise exceptions.StopEtlPipelineErrorException(e)
		
	if response.status_code == 204:
		logging.info('got here')
		logging.warn(f'course search index already exists, successful deletion prior to recreation\n\
			index: {index_name}')
	elif response.status_code != 404:
        # 404 is the expected response when deleting a course search index
		logging.error(f'unexpected response when deleting existing search index: {response.json()}\n\
			index-name: {index_name}\nstatus: {response.status_code}')

		raise exceptions.StopEtlPipelineErrorException

def create_index(url, headers, index_schema, index_name):
	try:
		response = requests.post(url, headers=headers, json=index_schema)
	except requests.exceptions.RequestException as e:
		logging.exception('unexpected error creating index',exc_info=True)
		raise exceptions.StopEtlPipelineErrorException(e)

	if response.status_code != 201:
		logging.error(f'failed to create search index\n\
                        index-name: {index_name}\nstatus: {response.status_code}')

		raise exceptions.StopEtlPipelineErrorException

def load_course_documents(course_url, headers, index_name, docs):
		number_of_docs = len(docs)
		course_count = 0
		bulk_course_count = 500
		documents = {}
		search_courses = []
		for doc in docs:
			course_count += 1
			
			search_course = models.build_course_search_doc(doc)
			search_courses.append(search_course)
			
			if course_count % bulk_course_count == 0 or course_count == number_of_docs:
				documents['value'] = search_courses
				
				bulk_create_courses(course_url, headers, documents, index_name)
				
				logging.info(f'successfully loaded {course_count} courses into azure search\n\
                        index: {index_name}\n')
							
				# Empty variables
				documents = {}
				search_courses = []

def bulk_create_courses(course_url, headers, documents, index_name):
	try:
		response = requests.post(course_url, headers=headers, json=documents)
	except requests.exceptions.RequestException as e:
		logging.exception('unexpected error creating index',exc_info=True)
		raise exceptions.StopEtlPipelineErrorException(e)

	if response.status_code != 200:
		logging.error(f'failed to bulk load course search documents\n\
                        index-name: {index_name}\nstatus: {response.status_code}')

		raise exceptions.StopEtlPipelineErrorException


def get_index_schema(index):
    course_schema = {
		"name": f'{index}',
		"fields": [{
			"name": "id",
			"type": "Edm.String",
			"key": "true"
		},
		{
			"name": "course",
			"type": "Edm.ComplexType",
			"fields": [{
					"name": "country",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"facetable": "false",
							"filterable": "true",
							"searchable": "false",
							"sortable": "false"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "false",
							"searchable": "false",
							"sortable": "false"
						}
					]
				},
				{
					"name": "distance_learning",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"facetable": "false",
							"filterable": "true",
							"searchable": "false",
							"sortable": "false"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "false",
							"searchable": "false",
							"sortable": "false"
						}
					]
				},
				{
					"name": "foundation_year_availability",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"facetable": "false",
							"filterable": "true",
							"searchable": "false",
							"sortable": "false"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "false",
							"searchable": "false",
							"sortable": "false"
						}
					]
				},
				{
					"name": "honours_award_provision",
					"type": "Edm.String",
					"facetable": "true",
					"filterable": "true",
					"searchable": "false",
					"sortable": "false"
				},
				{
					"name": "institution",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "pub_ukprn_name",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "true",
							"searchable": "true",
							"sortable": "false"
						},
						{
							"name": "sort_pub_ukprn_name",
							"type": "Edm.String",
							"facetable": "false",
							"filterable": "false",
							"searchable": "false",
							"sortable": "true"
						},
						{
							"name": "pub_ukprn",
							"type": "Edm.String",
							"facetable": "false",
							"filterable": "true",
							"searchable": "false",
							"sortable": "false"
						}
					]
				},
				{
					"name": "kis_course_id",
					"type": "Edm.String",
					"facetable": "false",
					"filterable": "false",
					"searchable": "false",
					"sortable": "false"
				},
				{
					"name": "length_of_course",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "true",
							"searchable": "false",
							"sortable": "false"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "false",
							"searchable": "false",
							"sortable": "false"
						}
					]
				},
				{
					"name": "locations",
					"type": "Collection(Edm.ComplexType)",
					"fields": [{
							"name": "geo",
							"type": "Edm.GeographyPoint",
							"facetable": "false",
							"filterable": "true",
							"searchable": "false",
							"sortable": "false"
						},
                        {
						"name": "name",
						"type": "Edm.ComplexType",
						"fields": [{
								"name": "english",
								"type": "Edm.String",
								"facetable": "true",
								"filterable": "false",
								"searchable": "false",
								"sortable": "false"
							},
							{
								"name": "welsh",
								"type": "Edm.String",
								"facetable": "true",
								"filterable": "false",
								"searchable": "false",
								"sortable": "false"
							}
						]
					}]
				},
				{
					"name": "mode",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "true",
							"searchable": "false",
							"sortable": "false"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "false",
							"searchable": "false",
							"sortable": "false"
						}
					]
				},
				{
					"name": "qualification",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "true",
							"searchable": "false",
							"sortable": "false"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "false",
							"searchable": "false",
							"sortable": "false"
						}
					]
				},
				{
					"name": "sandwich_year",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"facetable": "false",
							"filterable": "true",
							"searchable": "false",
							"sortable": "false"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "false",
							"searchable": "false",
							"sortable": "false"
						}
					]
				},
				{
					"name": "title",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "english",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "false",
							"searchable": "true",
							"sortable": "false"
						},
						{
							"name": "welsh",
							"type": "Edm.String",
							"facetable": "true",
							"filterable": "false",
							"searchable": "true",
							"sortable": "false"
						}
					]
				},
				{
					"name": "year_abroad",
					"type": "Edm.ComplexType",
					"fields": [{
						"name": "code",
						"type": "Edm.String",
						"facetable": "false",
						"filterable": "true",
						"searchable": "false",
						"sortable": "false"
					}, {
						"name": "label",
						"type": "Edm.String",
						"facetable": "true",
						"filterable": "false",
						"searchable": "false",
						"sortable": "false"
					}]
				}
			]
		}]
	}

    return course_schema