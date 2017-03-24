# -*- coding: utf-8 -*-
# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import datetime

import pytest
import tests.utils

from pyoozie import xml
from pyoozie import tags


@pytest.fixture
def workflow_app_path():
    return '/user/oozie/workflows/descriptive-name'


@pytest.fixture
def coord_app_path():
    return '/user/oozie/coordinators/descriptive-name'


@pytest.fixture
def username():
    return 'test'


@pytest.fixture
def workflow_builder():
    return xml.WorkflowBuilder(
        name='descriptive-name'
    ).add_action(
        name='payload',
        action=tags.Shell(exec_command='echo "test"'),
        action_on_error=tags.Email(to='person@example.com', subject='Error', body='A bad thing happened'),
        kill_on_error='Failure message 😢',
    )


def test_workflow_submission_xml(username, workflow_app_path):
    actual = xml._workflow_submission_xml(
        username=username,
        workflow_xml_path=workflow_app_path,
        indent=True,
    )
    assert tests.utils.xml_to_dict_unordered('''
    <configuration>
        <property>
            <name>oozie.wf.application.path</name>
            <value>/user/oozie/workflows/descriptive-name</value>
        </property>
        <property>
            <name>user.name</name>
            <value>test</value>
        </property>
    </configuration>''') == tests.utils.xml_to_dict_unordered(actual)


def test_workflow_submission_xml_with_configuration(username, workflow_app_path):
    actual = xml._workflow_submission_xml(
        username=username,
        workflow_xml_path=workflow_app_path,
        configuration={
            'other.key': 'other value',
        },
        indent=True
    )

    assert tests.utils.xml_to_dict_unordered('''
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
    </configuration>''') == tests.utils.xml_to_dict_unordered(actual)


def test_coordinator_submission_xml(username, coord_app_path):
    actual = xml._coordinator_submission_xml(
        username=username,
        coord_xml_path=coord_app_path,
        indent=True
    )
    assert tests.utils.xml_to_dict_unordered('''
    <configuration>
        <property>
            <name>oozie.coord.application.path</name>
            <value>/user/oozie/coordinators/descriptive-name</value>
        </property>
        <property>
            <name>user.name</name>
            <value>test</value>
        </property>
    </configuration>''') == tests.utils.xml_to_dict_unordered(actual)


def test_coordinator_submission_xml_with_configuration(username, coord_app_path):
    actual = xml._coordinator_submission_xml(
        username=username,
        coord_xml_path=coord_app_path,
        configuration={
            'oozie.coord.group.name': 'descriptive-group',
        },
        indent=True
    )
    assert tests.utils.xml_to_dict_unordered('''
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
    </configuration>''') == tests.utils.xml_to_dict_unordered(actual)


def test_workflow_builder(workflow_builder):
    with open('tests/data/workflow.xml', 'r') as fh:
        expected_xml = fh.read()

    # Is this XML expected
    actual_xml = workflow_builder.build()
    assert tests.utils.xml_to_dict_unordered(expected_xml) == tests.utils.xml_to_dict_unordered(actual_xml)

    # Does it validate against the workflow XML schema?
    tests.utils.assert_valid_workflow(actual_xml)


def test_builder_raises_on_bad_workflow_name():
    # Does it throw an exception on a bad workflow name?
    with pytest.raises(AssertionError) as assertion_info:
        xml.WorkflowBuilder(
            name='l' * (tags.MAX_NAME_LENGTH + 1)
        ).add_action(
            name='payload',
            action=tags.Shell(exec_command='echo "test"'),
            action_on_error=tags.Email(to='person@example.com', subject='Error', body='A bad thing happened'),
            kill_on_error='Failure message',
        )
    assert "Name must be less than " in str(assertion_info.value)


def test_builder_raises_on_bad_action_name():
    # Does it throw an exception on a bad action name?
    with pytest.raises(AssertionError) as assertion_info:
        xml.WorkflowBuilder(
            name='descriptive-name'
        ).add_action(
            name='Action name with invalid characters',
            action=tags.Shell(exec_command='echo "test"'),
            action_on_error=tags.Email(to='person@example.com', subject='Error', body='A bad thing happened'),
            kill_on_error='Failure message',
        )
    assert "Identifier must match " in str(assertion_info.value) and \
        "Action name with invalid characters" in str(assertion_info.value)

    # Does it throw an exception on an action name that's too long?
    with pytest.raises(AssertionError) as assertion_info:
        xml.WorkflowBuilder(
            name='descriptive-name'
        ).add_action(
            name='l' * (tags.MAX_IDENTIFIER_LENGTH + 1),
            action=tags.Shell(exec_command='echo "test"'),
            action_on_error=tags.Email(to='person@example.com', subject='Error', body='A bad thing happened'),
            kill_on_error='Failure message',
        )
    assert "Identifier must be less than " in str(assertion_info.value)


def test_builder_raises_on_multiple_actions(workflow_builder):
    # Does it raise an exception when you try to add multiple actions?
    with pytest.raises(NotImplementedError) as assertion_info:
        workflow_builder.add_action(
            name='payload',
            action=tags.Shell(exec_command='echo "test"'),
            action_on_error=tags.Email(to='person@example.com', subject='Error', body='A bad thing happened'),
            kill_on_error='Failure message',
        )
    assert str(assertion_info.value) == 'Can only add one action in this version'


def test_coordinator_builder(coordinator_xml_with_controls, workflow_app_path):

    coord_builder = xml.CoordinatorBuilder(
        name='coordinator-name',
        workflow_xml_path=workflow_app_path,
        frequency_in_minutes=24 * 60,  # In minutes
        start=datetime.datetime(2015, 1, 1, 10, 56),
        end=datetime.datetime(2115, 1, 1, 10, 56),
        concurrency=1,
        throttle='${throttle}',
        timeout_in_minutes=10,
        execution_order=tags.EXEC_LAST_ONLY,
        parameters={
            'throttle': 1,
        },
        workflow_configuration={
            'mapred.job.queue.name': 'production',
        })

    # Can it XML?
    expected_xml = coord_builder.build()
    actual_dict = tests.utils.xml_to_dict_unordered(coordinator_xml_with_controls)
    expected_dict = tests.utils.xml_to_dict_unordered(expected_xml)
    assert actual_dict == expected_dict
