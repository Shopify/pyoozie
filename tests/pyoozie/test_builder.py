# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a BSD-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

from datetime import datetime

import pytest

from mock import Mock

from tests.utils import xml_to_dict_unordered
from pyoozie import workflow, coordinator, Shell, Email, ExecutionOrder, _workflow_submission_xml, \
    _coordinator_submission_xml


@pytest.fixture
def workflow_builder():
    return workflow(
        name='descriptive-name'
    ).add_action(
        name='payload',
        action=Shell(exec_command='echo "test"'),
        action_on_error=Email(to='person@example.com', subject='Error', body='A bad thing happened'),
        kill_on_error='Failure message',
    )


@pytest.fixture
def workflow_app_path():
    return '/user/oozie/workflows/descriptive-name'


@pytest.fixture
def coord_app_path():
    return '/user/oozie/coordinators/descriptive-name'


@pytest.fixture
def hadoop_user():
    return 'test'


def test_workflow_submission_xml(hadoop_user, workflow_app_path):
    actual = _workflow_submission_xml(
        hadoop_user=hadoop_user,
        hdfs_path=workflow_app_path,
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


def test_workflow_submission_xml_with_configuration(hadoop_user, workflow_app_path):
    actual = _workflow_submission_xml(
        hadoop_user=hadoop_user,
        hdfs_path=workflow_app_path,
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


def test_coordinator_submission_xml(hadoop_user, coord_app_path):
    actual = _coordinator_submission_xml(
        hadoop_user=hadoop_user,
        hdfs_path=coord_app_path,
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


def test_coordinator_submission_xml_with_configuration(hadoop_user, coord_app_path):
    actual = _coordinator_submission_xml(
        hadoop_user=hadoop_user,
        hdfs_path=coord_app_path,
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


def test_workflow_builder(workflow_builder, hadoop_user, workflow_app_path):
    with open('tests/data/workflow.xml', 'r') as fh:
        expected = fh.read()

    # Can it XML?
    actual_xml = workflow_builder.build()
    assert xml_to_dict_unordered(expected) == xml_to_dict_unordered(actual_xml)

    # Can it dance with Oozie? (not quite)
    mock_hdfs_callback = Mock()
    with pytest.raises(NotImplementedError):
        workflow_builder.submit(oozie_url='https://my.oozie.server',
                                hadoop_user=hadoop_user,
                                hdfs_path=workflow_app_path,
                                hdfs_callback=mock_hdfs_callback)
    mock_hdfs_callback.assert_called_once_with(workflow_app_path, actual_xml)


def test_coordinator_builder(coordinator_xml_with_controls, workflow_builder, workflow_app_path, coord_app_path,
                             hadoop_user):

    coord_builder = coordinator(
        name='coordinator-name',
        frequency_in_minutes=24 * 60,  # In minutes
        start=datetime(2015, 1, 1, 10, 56),
        end=datetime(2115, 1, 1, 10, 56),
        concurrency=1,
        throttle='${throttle}',
        timeout_in_minutes=10,
        execution_order=ExecutionOrder.LAST_ONLY,
        workflow=workflow_builder,
        parameters={
            'throttle': 1,
        },
        workflow_configuration={
            'mapred.job.queue.name': 'production',
        })

    # Can it XML?
    expected_xml = coord_builder.build(workflow_app_path)
    assert xml_to_dict_unordered(coordinator_xml_with_controls) == xml_to_dict_unordered(expected_xml)

    # Can it dance with Oozie? (not quite)

    mock_hdfs_callback = Mock()
    with pytest.raises(NotImplementedError):
        coord_builder.submit(oozie_url='https://my.oozie.server',
                             workflow_hdfs_path=workflow_app_path,
                             coord_hdfs_path=coord_app_path,
                             hadoop_user=hadoop_user,
                             hdfs_callback=mock_hdfs_callback,
                             timeout_in_seconds=5,
                             verbose=True)
    mock_hdfs_callback.assert_called_once_with(workflow_app_path, workflow_builder.build())
    # TODO test that we also got to the point where we used the callback to store coordinator XML
