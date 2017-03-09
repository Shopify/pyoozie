# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
import datetime

import pytest
import tests.utils

from pyoozie import coordinator
from pyoozie import tags


def parse_datetime(string):
    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%MZ')


@pytest.fixture
def expected_coordinator_options():
    return {
        'name': 'coordinator-name',
        'frequency': 1440,
        'start': parse_datetime('2015-01-01T10:56Z'),
        'end': parse_datetime('2115-01-01T10:56Z'),
        'workflow_app_path': '/user/oozie/workflows/descriptive-name',
    }


@pytest.fixture
def coordinator_xml():
    with open('tests/data/coordinator.xml', 'r') as fh:
        return fh.read()


def test_coordinator(coordinator_xml, expected_coordinator_options):
    actual = coordinator.Coordinator(**expected_coordinator_options).xml()
    assert tests.utils.xml_to_dict_unordered(coordinator_xml) == tests.utils.xml_to_dict_unordered(actual)


def test_coordinator_end_default(coordinator_xml, expected_coordinator_options):
    del expected_coordinator_options['end']
    actual = coordinator.Coordinator(**expected_coordinator_options).xml()
    assert tests.utils.xml_to_dict_unordered(coordinator_xml) == tests.utils.xml_to_dict_unordered(actual)


def test_coordinator_with_controls_and_more(coordinator_xml_with_controls, expected_coordinator_options):
    actual = coordinator.Coordinator(
        timeout=10,
        concurrency=1,
        execution_order=coordinator.ExecutionOrder.LAST_ONLY,
        throttle='${throttle}',
        workflow_configuration=tags.Configuration({
            'mapred.job.queue.name': 'production'
        }),
        parameters=tags.Parameters({
            'throttle': 1
        }),
        **expected_coordinator_options
    ).xml()
    assert tests.utils.xml_to_dict_unordered(coordinator_xml_with_controls) == \
        tests.utils.xml_to_dict_unordered(actual)


def test_really_long_coordinator_name(expected_coordinator_options):
    with pytest.raises(AssertionError) as assertion_info:
        del expected_coordinator_options['name']
        coordinator.Coordinator(name='l' * (tags.MAX_NAME_LENGTH + 1), **expected_coordinator_options)
    assert "Name must be less than" in str(assertion_info.value)


def test_coordinator_bad_frequency(expected_coordinator_options):
    expected_coordinator_options['frequency'] = 0
    with pytest.raises(AssertionError) as assertion_info:
        coordinator.Coordinator(**expected_coordinator_options)
    assert str(assertion_info.value) == \
        'Frequency (0 min) must be greater than or equal to 5 min'


def test_coordinator_end_before_start(expected_coordinator_options):
    expected_coordinator_options['end'] = expected_coordinator_options['start'] - datetime.timedelta(days=10)
    with pytest.raises(AssertionError) as assertion_info:
        coordinator.Coordinator(**expected_coordinator_options)
    assert str(assertion_info.value) == \
        'End time (2014-12-22T10:56Z) must be greater than the start time (2015-01-01T10:56Z)'
