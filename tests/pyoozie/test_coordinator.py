# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from datetime import datetime, timedelta

import pytest

from pyoozie.coordinator import Coordinator, ExecutionOrder, Configuration, Parameters
from tests.utils import xml_to_dict_unordered


def parse_datetime(string):
    return datetime.strptime(string, '%Y-%m-%dT%H:%MZ')


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
    actual = Coordinator(**expected_coordinator_options).xml()
    assert xml_to_dict_unordered(coordinator_xml) == xml_to_dict_unordered(actual)


def test_coordinator_end_default(coordinator_xml, expected_coordinator_options):
    del expected_coordinator_options['end']
    actual = Coordinator(**expected_coordinator_options).xml()
    assert xml_to_dict_unordered(coordinator_xml) == xml_to_dict_unordered(actual)


def test_coordinator_with_controls_and_more(coordinator_xml_with_controls, expected_coordinator_options):
    actual = Coordinator(
        timeout=10,
        concurrency=1,
        execution_order=ExecutionOrder.LAST_ONLY,
        throttle='${throttle}',
        workflow_configuration=Configuration({
            'mapred.job.queue.name': 'production'
        }),
        parameters=Parameters({
            'throttle': 1
        }),
        **expected_coordinator_options
    ).xml()
    assert xml_to_dict_unordered(coordinator_xml_with_controls) == xml_to_dict_unordered(actual)


def test_really_long_coordinator_name(expected_coordinator_options):
    with pytest.raises(AssertionError) as assertion_info:
        del expected_coordinator_options['name']
        Coordinator(name='long' * 10, **expected_coordinator_options)
    assert str(assertion_info.value) == \
        "Identifier must be less than 39 chars long, 'longlonglonglonglonglonglonglonglonglong' is 40"


def test_coordinator_bad_frequency(expected_coordinator_options):
    expected_coordinator_options['frequency'] = 0
    with pytest.raises(AssertionError) as assertion_info:
        Coordinator(**expected_coordinator_options)
    assert str(assertion_info.value) == \
        'Frequency (0 min) must be greater than or equal to 5 min'


def test_coordinator_end_before_start(expected_coordinator_options):
    expected_coordinator_options['end'] = expected_coordinator_options['start'] - timedelta(days=10)
    with pytest.raises(AssertionError) as assertion_info:
        Coordinator(**expected_coordinator_options)
    assert str(assertion_info.value) == \
        'End time (2014-12-22T10:56Z) must be greater than the start time (2015-01-01T10:56Z)'
