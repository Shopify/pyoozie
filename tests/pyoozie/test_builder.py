# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import datetime
import subprocess

import pytest
import tests.utils

from pyoozie import builder
from pyoozie import tags
from pyoozie import transforms


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
    actual = builder._workflow_submission_xml(
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
    actual = builder._workflow_submission_xml(
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
    actual = builder._coordinator_submission_xml(
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
    actual = builder._coordinator_submission_xml(
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


@pytest.mark.xfail
def test_workflow_builder(tmpdir):

    workflow_builder = builder.WorkflowBuilder(
        name='descriptive-name',
        job_tracker='job-tracker',
        name_node='name-node',
    ).add_action(
        name='extract',
        action=tags.Shell(exec_command='echo', arguments=["'Extract data from an operational system'"]),
        action_on_error=tags.Email(to='person@example.com', subject='Error',
                                   body='A bad thing happened while extracting'),
        kill_on_error='Failure message on extracting',
    ).add_action(
        name='transform',
        action=tags.Shell(exec_command='echo', arguments=["'Transform data'"]),
        action_on_error=tags.Email(to='person@example.com', subject='Error',
                                   body='A bad thing happened while transforming'),
        kill_on_error='Failure message on transforming',
        depends_upon=('extract',),
    ).add_action(
        name='load',
        action=tags.Shell(exec_command='echo', arguments=["'Load data into a database'"]),
        action_on_error=tags.Email(to='person@example.com', subject='Error',
                                   body='A bad thing happened while loading'),
        kill_on_error='Failure message on loading',
        depends_upon=('transform',),
    ).add_transform(
        transforms.final_action,
        action=tags.Email(to='person@example.com', subject='Success', body='ETL succeeded'),
    ).add_transform(
        transforms.add_actions_on_error
    )


    expected_xml = """
<?xml version='1.0' encoding='UTF-8'?>
<workflow-app xmlns="uri:oozie:workflow:0.5" name="descriptive-name">
    <global>
        <job-tracker>job-tracker</job-tracker>
        <name-node>name-node</name-node>
    </global>
</workflow-app>""".strip()

    # Is this XML expected
    actual_xml = workflow_builder.build(indent=True)
    assert tests.utils.xml_to_dict_unordered(expected_xml) == tests.utils.xml_to_dict_unordered(actual_xml)

    # Does it validate against the workflow XML schema?
    filename = tmpdir.join("workflow.xml")
    filename.write_text(actual_xml, encoding='utf8')
    try:
        subprocess.check_output(
            'java -cp lib/oozie-client-4.1.0.jar:lib/commons-cli-1.2.jar '
            'org.apache.oozie.cli.OozieCLI validate {path}'.format(path=str(filename)),
            stderr=subprocess.STDOUT,
            shell=True
        )
    except subprocess.CalledProcessError as e:
        raise AssertionError('An XML validation error\n\n{error}\n\noccurred while parsing:\n\n{xml}'.format(
            error=e.output.decode('utf8').strip(),
            xml=actual_xml,
        ))


def test_builder_raises_on_bad_workflow_name():
    # Does it throw an exception on a bad workflow name?
    with pytest.raises(AssertionError) as assertion_info:
        builder.WorkflowBuilder(
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
        builder.WorkflowBuilder(
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
        builder.WorkflowBuilder(
            name='descriptive-name'
        ).add_action(
            name='l' * (tags.MAX_IDENTIFIER_LENGTH + 1),
            action=tags.Shell(exec_command='echo "test"'),
            action_on_error=tags.Email(to='person@example.com', subject='Error', body='A bad thing happened'),
            kill_on_error='Failure message',
        )
    assert "Identifier must be less than " in str(assertion_info.value)


def test_coordinator_builder(coordinator_xml_with_controls, workflow_app_path):

    coord_builder = builder.CoordinatorBuilder(
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
