
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
							"filterable": "true"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"retrievable": "true"
						}
					]
				},
				{
					"name": "distance_learning",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"filterable": "true"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"retrievable": "true"
						}
					]
				},
				{
					"name": "foundation_year_availability",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"filterable": "true"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"retrievable": "true"
						}
					]
				},
				{
					"name": "honours_award_provision",
					"type": "Edm.String",
					"filterable": "true",
					"retrievable": "true"
				},
				{
					"name": "institution",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "pub_ukprn_name",
							"type": "Edm.String",
							"filterable": "true",
							"searchable": "true",
							"retrievable": "true"
						},
						{
							"name": "sort_pub_ukprn_name",
							"type": "Edm.String",
							"sortable": "true"
						},
						{
							"name": "pub_ukprn",
							"type": "Edm.String",
							"filterable": "true"
						}
					]
				},
				{
					"name": "kis_course_id",
					"type": "Edm.String",
					"retrievable": "true"
				},
				{
					"name": "length_of_course",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"filterable": "true"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"retrievable": "true"
						}
					]
				},
				{
					"name": "locations",
					"type": "Collection(Edm.ComplexType)",
					"fields": [{
							"name": "geo",
							"type": "Edm.GeographyPoint",
							"filterable": "true"
						},
                        {
						"name": "name",
						"type": "Edm.ComplexType",
						"fields": [{
								"name": "english",
								"type": "Edm.String",
								"retrievable": "true"
							},
							{
								"name": "welsh",
								"type": "Edm.String",
								"retrievable": "true"
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
							"filterable": "true"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"retrievable": "true"
						}
					]
				},
				{
					"name": "qualification",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"filterable": "true"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"retrievable": "true"
						}
					]
				},
				{
					"name": "sandwich_year",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "code",
							"type": "Edm.String",
							"filterable": "true"
						},
						{
							"name": "label",
							"type": "Edm.String",
							"retrievable": "true"
						}
					]
				},
				{
					"name": "title",
					"type": "Edm.ComplexType",
					"fields": [{
							"name": "english",
							"type": "Edm.String",
							"searchable": "true",
							"facetable": "true",
							"retrievable": "true"
						},
						{
							"name": "welsh",
							"type": "Edm.String",
							"searchable": "true",
							"facetable": "true",
							"retrievable": "true"
						}
					]
				},
				{
					"name": "year_abroad",
					"type": "Edm.ComplexType",
					"fields": [{
						"name": "code",
						"type": "Edm.String",
						"filterable": "true"
					}, {
						"name": "label",
						"type": "Edm.String",
						"retrievable": "true"
					}]
				}
			]
		}
	]
}

    return course_schema