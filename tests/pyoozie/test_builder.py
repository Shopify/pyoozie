# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a BSD-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

from datetime import datetime

import pytest

from tests.utils import xml_to_dict_unordered
from pyoozie import WorkflowBuilder, CoordinatorBuilder, Shell, Email, ExecutionOrder
from pyoozie.builder import _workflow_submission_xml, _coordinator_submission_xml


@pytest.fixture
def workflow_app_path():
    return '/user/oozie/workflows/descriptive-name'


@pytest.fixture
def coord_app_path():
    return '/user/oozie/coordinators/descriptive-name'


@pytest.fixture
def username():
    return 'test'


def test_workflow_submission_xml(username, workflow_app_path):
    actual = _workflow_submission_xml(
        username=username,
        workflow_xml_path=workflow_app_path,
        indent=True,
    )
    assert xml_to_dict_unordered('''
    <configuration>
        <property>
            <name>oozie.wf.application.path</name>
            <value>/user/oozie/workflows/descriptive-name</value>
        </property>
        <property>
            <name>user.name</name>
            <value>test</value>
        </property>
    </configuration>''') == xml_to_dict_unordered(actual)


def test_workflow_submission_xml_with_configuration(username, workflow_app_path):
    actual = _workflow_submission_xml(
        username=username,
        workflow_xml_path=workflow_app_path,
        configuration={
            'other.key': 'other value',
        },
        indent=True
    )

    assert xml_to_dict_unordered('''
    <configuration>
        <property>
            <name>other.key</name>
            <value>other value</value>
        </property>
        <property>
            <name>oozie.wf.application.path</name>
            <value>/user/oozie/workflows/descriptive-name</value>
        </property>
        <property>
            <name>user.name</name>
            <value>test</value>
        </property>
    </configuration>''') == xml_to_dict_unordered(actual)


def test_coordinator_submission_xml(username, coord_app_path):
    actual = _coordinator_submission_xml(
        username=username,
        coord_xml_path=coord_app_path,
        indent=True
    )
    assert xml_to_dict_unordered('''
    <configuration>
        <property>
            <name>oozie.coord.application.path</name>
            <value>/user/oozie/coordinators/descriptive-name</value>
        </property>
        <property>
            <name>user.name</name>
            <value>test</value>
        </property>
    </configuration>''') == xml_to_dict_unordered(actual)


def test_coordinator_submission_xml_with_configuration(username, coord_app_path):
    actual = _coordinator_submission_xml(
        username=username,
        coord_xml_path=coord_app_path,
        configuration={
            'oozie.coord.group.name': 'descriptive-group',
        },
        indent=True
    )
    assert xml_to_dict_unordered('''
    <configuration>
        <property>
            <name>oozie.coord.application.path</name>
            <value>/user/oozie/coordinators/descriptive-name</value>
        </property>
        <property>
            <name>oozie.coord.group.name</name>
            <value>descriptive-group</value>
        </property>
        <property>
            <name>user.name</name>
            <value>test</value>
        </property>
    </configuration>''') == xml_to_dict_unordered(actual)


def test_workflow_builder():
    with open('tests/data/workflow.xml', 'r') as fh:
        expected = fh.read()

    # Can it XML?
    builder = WorkflowBuilder(
        name='descriptive-name'
    ).add_action(
        name='payload',
        action=Shell(exec_command='echo "test"'),
        action_on_error=Email(to='person@example.com', subject='Error', body='A bad thing happened'),
        kill_on_error='Failure message',
    )

    actual_xml = builder.build()
    assert xml_to_dict_unordered(expected) == xml_to_dict_unordered(actual_xml)


def test_coordinator_builder(coordinator_xml_with_controls, workflow_app_path):

    builder = CoordinatorBuilder(
        name='coordinator-name',
        workflow_xml_path=workflow_app_path,
        frequency_in_minutes=24 * 60,  # In minutes
        start=datetime(2015, 1, 1, 10, 56),
        end=datetime(2115, 1, 1, 10, 56),
        concurrency=1,
        throttle='${throttle}',
        timeout_in_minutes=10,
        execution_order=ExecutionOrder.LAST_ONLY,
        parameters={
            'throttle': 1,
        },
        workflow_configuration={
            'mapred.job.queue.name': 'production',
        })

    # Can it XML?
    expected_xml = builder.build()
    assert xml_to_dict_unordered(coordinator_xml_with_controls) == xml_to_dict_unordered(expected_xml)
