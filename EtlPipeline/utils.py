from EtlPipeline.course_stats import SharedUtils

# TODO: **House-keeping** review why this is setup this way

g_subject_enricher = None


def get_subject(subject_code):
    subject = {}
    subject["code"] = subject_code
    subject["english_label"] = g_subject_enricher.subject_lookups[subject_code]["english_name"]
    subject["welsh_label"] = g_subject_enricher.subject_lookups[subject_code]["welsh_name"]
    return subject


def get_go_work_unavail_messages(xml_element_key, xml_agg_key, xml_unavail_reason_key, raw_data_element):
    shared_utils = SharedUtils(
        xml_element_key,
        "GOWORKSBJ",
        xml_agg_key,
        xml_unavail_reason_key,
    )
    return shared_utils.get_unavailable(raw_data_element)


def get_earnings_agg_unavail_messages(agg_value, subject):
    earnings_agg_unavail_messages = {}

    if agg_value in ['21', '22']:
        message_english = "The data displayed is from students on this and other " \
                          "courses in [Subject].\n\nThis includes data from this and related courses at the same university or " \
                          "college. There was not enough data to publish more specific information. This does not reflect on " \
                          "the quality of the course."
        message_welsh = "Daw'r data a ddangosir gan fyfyrwyr ar y cwrs hwn a chyrsiau " \
                        "[Subject] eraill.\n\nMae hwn yn cynnwys data o'r cwrs hwn a chyrsiau cysylltiedig yn yr un brifysgol " \
                        "neu goleg. Nid oedd digon o ddata ar gael i gyhoeddi gwybodaeth fwy manwl. Nid yw hyn yn adlewyrchu " \
                        "ansawdd y cwrs."

        earnings_agg_unavail_messages['english'] = message_english.replace("[Subject]", subject['english_label'])
        earnings_agg_unavail_messages['welsh'] = message_welsh.replace("[Subject]", subject['welsh_label'])

    return earnings_agg_unavail_messages
