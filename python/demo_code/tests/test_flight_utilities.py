import flight_utilities as sut
import pytest


def test_get_year_range_2016_2020():
    # given
    sut_input = 'Carrier (2016 - 2020)'
    expected = [2016, 2020]

    # when
    actual = sut.get_year_range(sut_input)

    # then
    assert expected == actual


def test_get_year_range_2016_blank():
    # given
    sut_input = 'Carrier (2016 - )'
    expected = [2016, None]

    # when
    actual = sut.get_year_range(sut_input)

    # then
    assert expected == actual


def test_get_year_range_blank_2016():
    # given
    sut_input = 'Carrier ( - 2016)'
    expected = [None, 2016]

    # when
    actual = sut.get_year_range(sut_input)

    # then
    assert expected == actual


def test_get_year_range_multiple_left_parentheses():
    # given
    sut_input = 'Carrier (carrier) (2010 - 2016)'
    expected = [2010, 2016]

    # when
    actual = sut.get_year_range(sut_input)

    # then
    assert expected == actual


def test_get_year_range_no_years():
    # given
    sut_input = 'Carrier ( - )'
    expected = [None, None]

    # when
    actual = sut.get_year_range(sut_input)

    # then
    assert expected == actual


def test_get_year_range_no_range():
    # given
    sut_input = 'Carrier'

    # when / then (exception)
    with pytest.raises(ValueError, match=r"No year range was found in ."):
        actual = sut.get_year_range(sut_input)


def test_get_year_range_no_right_parenthesis():
    # given
    sut_input = 'Carrier (2016 - 2022'

    # when / then (exception)
    with pytest.raises(ValueError, match=r"No year range was found in ."):
        actual = sut.get_year_range(sut_input)


def test_get_year_range_no_left_parenthesis():
    # given
    sut_input = 'Carrier 2016 - 2022)'

    # when / then (exception)
    with pytest.raises(ValueError, match=r"No year range was found in ."):
        actual = sut.get_year_range(sut_input)