from course_search_build.build_institutions_json import get_inst_entry


def test_get_inst_entry():
    name = "test name"
    first_trading_name = ""
    legal_name = ""
    other_names = ""
    entry = get_inst_entry(name, first_trading_name, legal_name, other_names)
    assert entry == {
        'alphabet': 't',
        'first_trading_name': first_trading_name,
        'legal_name': legal_name,
        'name': 'test name',
        'order_by_name': 'test name',
        'other_names': other_names
    }
