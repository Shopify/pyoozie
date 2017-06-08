# -*- coding: utf-8 -*-
# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals
from __future__ import print_function

import datetime
import decimal

import pytest
import six
import tests.utils

from pyoozie import tags


pytestmark = pytest.mark.usefixtures("fake_uuid4")  # pylint: disable=global-variable


@pytest.fixture
def expected_property_values():
    return {
        'boolean': False,
        'decimal': decimal.Decimal('0.75'),
        'float': 0.5,
        'int': 0,
        'long': 10,
        'none': None,
        'unicode': 'ǝnlɐʌ',
        'string': str('value'),
    }


@pytest.fixture
def expected_property_values_xml():
    return '''
        <property>
            <name>boolean</name>
            <value>False</value>
        </property>
        <property>
            <name>decimal</name>
            <value>0.75</value>
        </property>
        <property>
            <name>float</name>
            <value>0.5</value>
        </property>
        <property>
            <name>int</name>
            <value>0</value>
        </property>
        <property>
            <name>long</name>
            <value>10</value>
        </property>
        <property>
            <name>none</name>
            <value></value>
        </property>
        <property>
            <name>string</name>
            <value>value</value>
        </property>
        <property>
            <name>unicode</name>
            <value>ǝnlɐʌ</value>
        </property>'''


@pytest.fixture
def minimal_coordinator_options():
    return {
        'name': 'coordinator-name',
        'workflow_app_path': '/user/oozie/workflows/descriptive-name',
        'frequency': 1440,
        'start': parse_datetime('2015-01-01T10:56Z'),
    }


def test_validate_xml_id():
    # Simple id
    tags.validate_xml_id('ok-id')

    # Max-sized id
    tags.validate_xml_id('l' * tags.MAX_IDENTIFIER_LENGTH)


def test_validate_xml_id_thats_too_long():
    # Id that is too long
    very_long_name = 'l' * (tags.MAX_IDENTIFIER_LENGTH + 1)
    with pytest.raises(AssertionError) as assertion_info:
        tags.validate_xml_id(very_long_name)
    assert str(assertion_info.value) == (
        "Identifier must be less than {max_length} chars long, '{identifier}' is {length}"
    ).format(
        max_length=tags.MAX_IDENTIFIER_LENGTH,
        identifier=very_long_name,
        length=len(very_long_name)
    )


def test_validate_xml_id_with_illegal_start_char():
    # Id that doesn't satisfy regex because of bad start char
    with pytest.raises(AssertionError) as assertion_info:
        tags.validate_xml_id('0-id-starting-with-a-non-alpha-char')
    assert str(assertion_info.value) == (
        "Identifier must match {regex}, '0-id-starting-with-a-non-alpha-char' does not"
    ).format(regex=tags.REGEX_IDENTIFIER)


def test_validate_xml_id_with_illegal_char():
    # Id that doesn't satisfy regex because of illegal char
    with pytest.raises(AssertionError) as assertion_info:
        tags.validate_xml_id('id.with.illlegal.chars')
    assert str(assertion_info.value) == (
        "Identifier must match {regex}, 'id.with.illlegal.chars' does not"
    ).format(regex=tags.REGEX_IDENTIFIER)


def test_validate_xml_name():
    # Simple name
    tags.validate_xml_name('OK name (with punctuation)')

    # Max-sized name
    tags.validate_xml_name('l' * tags.MAX_NAME_LENGTH)


def test_validate_xml_name_thats_too_long():
    # Name that is too long
    very_long_name = 'l' * (tags.MAX_NAME_LENGTH + 1)
    with pytest.raises(AssertionError) as assertion_info:
        tags.validate_xml_name(very_long_name)
    assert str(assertion_info.value) == (
        "Name must be less than {max_length} chars long, '{name}' is {length}"
    ).format(
        max_length=tags.MAX_NAME_LENGTH,
        name=very_long_name,
        length=len(very_long_name)
    )


def test_validate_xml_name_with_latin1_char():
    # Name with a latin-1 character
    name_with_non_ascii = 'être'
    with pytest.raises(AssertionError) as assertion_info:
        tags.validate_xml_name(name_with_non_ascii)
    assert six.text_type(assertion_info.value) == (
        "Name must be comprised of printable ASCII characters, '{name}' is not"
    ).format(name=name_with_non_ascii)


def test_xml_serializable():
    class MyTag(tags.XMLSerializable):

        def _xml(self, doc, tag, text):
            super(MyTag, self)._xml(doc, tag, text)

    my_tag = MyTag('tag')
    with pytest.raises(NotImplementedError):
        my_tag.xml()


def test_parameters(expected_property_values, expected_property_values_xml):
    actual = tags.Parameters(expected_property_values).xml(indent=True)
    expected = '<parameters>{xml}</parameters>'.format(xml=expected_property_values_xml)
    assert tests.utils.xml_to_comparable_dict(expected) == tests.utils.xml_to_comparable_dict(actual)


def test_configuration(expected_property_values, expected_property_values_xml):
    actual = tags.Configuration(expected_property_values).xml(indent=True)
    expected = '<configuration>{xml}</configuration>'.format(xml=expected_property_values_xml)
    assert tests.utils.xml_to_comparable_dict(expected) == tests.utils.xml_to_comparable_dict(actual)


def test_credential(expected_property_values, expected_property_values_xml):
    actual = tags.Credential(expected_property_values,
                             credential_name='my-hcat-creds',
                             credential_type='hcat').xml(indent=True)
    expected = "<credential name='my-hcat-creds' type='hcat'>{xml}</credential>".format(
        xml=expected_property_values_xml)
    assert tests.utils.xml_to_comparable_dict(expected) == tests.utils.xml_to_comparable_dict(actual)


def test_shell():
    actual = tags.Shell(
        exec_command='${EXEC}',
        job_tracker='${jobTracker}',
        name_node='${nameNode}',
        prepare=None,
        job_xml_files=['/user/${wf:user()}/job.xml'],
        configuration={
            'mapred.job.queue.name': '${queueName}'
        },
        arguments=['A', 'B'],
        env_vars={
            'ENVIRONMENT': 'production',
            'RESOURCES': 'large',
        },
        files=['/users/blabla/testfile.sh#testfile'],
        archives=['/users/blabla/testarchive.jar#testarchive'],
        capture_output=True,
    ).xml(indent=True)
    assert tests.utils.xml_to_comparable_dict('''
    <shell xmlns="uri:oozie:shell-action:0.3">
        <job-tracker>${jobTracker}</job-tracker>
        <name-node>${nameNode}</name-node>
        <job-xml>/user/${wf:user()}/job.xml</job-xml>
        <configuration>
            <property>
                <name>mapred.job.queue.name</name>
                <value>${queueName}</value>
            </property>
        </configuration>
        <exec>${EXEC}</exec>
        <argument>A</argument>
        <argument>B</argument>
        <file>/users/blabla/testfile.sh#testfile</file>
        <archive>/users/blabla/testarchive.jar#testarchive</archive>
        <env-var>ENVIRONMENT=production</env-var>
        <env-var>RESOURCES=large</env-var>
        <capture-output />
    </shell>''') == tests.utils.xml_to_comparable_dict(actual)

    # Test using prepare fails
    with pytest.raises(NotImplementedError) as assertion_info:
        tags.Shell(
            exec_command='${EXEC}',
            prepare=['anything'],
        ).xml()
    assert str(assertion_info.value) == "Shell action's prepare has not yet been implemented"


def test_subworkflow():
    app_path = '/user/username/workflows/cool-flow'

    actual = tags.SubWorkflow(
        app_path=app_path,
        propagate_configuration=False,
        configuration=None,
    ).xml(indent=True)
    assert tests.utils.xml_to_comparable_dict('''
    <sub-workflow>
        <app-path>/user/username/workflows/cool-flow</app-path>
    </sub-workflow>
    ''') == tests.utils.xml_to_comparable_dict(actual)

    actual = tags.SubWorkflow(
        app_path=app_path,
        propagate_configuration=True,
        configuration=None,
    ).xml(indent=True)
    assert tests.utils.xml_to_comparable_dict('''
    <sub-workflow>
        <app-path>/user/username/workflows/cool-flow</app-path>
        <propagate-configuration />
    </sub-workflow>
    ''') == tests.utils.xml_to_comparable_dict(actual)

    actual = tags.SubWorkflow(
        app_path=app_path,
        propagate_configuration=True,
        configuration={
            'job_tracker': 'a_jobtracker',
            'name_node': 'hdfs://localhost:50070',
        },
    ).xml(indent=True)
    assert tests.utils.xml_to_comparable_dict('''
    <sub-workflow>
        <app-path>/user/username/workflows/cool-flow</app-path>
        <propagate-configuration />
        <configuration>
            <property>
                <name>job_tracker</name>
                <value>a_jobtracker</value>
            </property>
            <property>
                <name>name_node</name>
                <value>hdfs://localhost:50070</value>
            </property>
        </configuration>
    </sub-workflow>
    ''') == tests.utils.xml_to_comparable_dict(actual)


def test_global_configuration():
    configuration = {
        'mapred.job.queue.name': '${queueName}'
    }

    actual = tags.GlobalConfiguration(
        job_tracker='a_jobtracker',
        name_node='hdfs://localhost:50070',
        job_xml_files=None,
        configuration=None,
    ).xml(indent=True)
    assert tests.utils.xml_to_comparable_dict('''
    <global>
        <job-tracker>a_jobtracker</job-tracker>
        <name-node>hdfs://localhost:50070</name-node>
    </global>
    ''') == tests.utils.xml_to_comparable_dict(actual)

    actual = tags.GlobalConfiguration(
        job_tracker='a_jobtracker',
        name_node='hdfs://localhost:50070',
        job_xml_files=['/user/${wf:user()}/job.xml'],
        configuration=configuration,
    ).xml(indent=True)
    assert tests.utils.xml_to_comparable_dict('''
    <global>
        <job-tracker>a_jobtracker</job-tracker>
        <name-node>hdfs://localhost:50070</name-node>
        <job-xml>/user/${wf:user()}/job.xml</job-xml>
        <configuration>
            <property>
                <name>mapred.job.queue.name</name>
                <value>${queueName}</value>
            </property>
        </configuration>
    </global>
    ''') == tests.utils.xml_to_comparable_dict(actual)


def test_email():
    actual = tags.Email(
        to='mrt@example.com',
        subject='Chains',
        body='Do you need more?',
    ).xml(indent=True)
    assert tests.utils.xml_to_comparable_dict('''
    <email xmlns="uri:oozie:email-action:0.2">
        <to>mrt@example.com</to>
        <subject>Chains</subject>
        <body>Do you need more?</body>
    </email>
    ''') == tests.utils.xml_to_comparable_dict(actual)

    actual = tags.Email(
        to='mrt@example.com',
        subject='Chains',
        body='Do you need more?',
        cc='ateam@example.com',
        bcc='jewelrystore@example.com',
        content_type='text/plain',
        attachments='/path/to/attachment/on/hdfs.txt',
    ).xml(indent=True)
    assert tests.utils.xml_to_comparable_dict('''
    <email xmlns="uri:oozie:email-action:0.2">
        <to>mrt@example.com</to>
        <subject>Chains</subject>
        <body>Do you need more?</body>
        <cc>ateam@example.com</cc>
        <bcc>jewelrystore@example.com</bcc>
        <content_type>text/plain</content_type>
        <attachment>/path/to/attachment/on/hdfs.txt</attachment>
    </email>
    ''') == tests.utils.xml_to_comparable_dict(actual)

    actual = tags.Email(
        to=['mrt@example.com', 'b.a.baracus@example.com'],
        subject='Chains',
        body='Do you need more?',
        cc=('ateam@example.com', 'webmaster@example.com'),
        bcc=set(['jewelrystore@example.com', 'goldchains4u@example.com']),
        content_type='text/plain',
        attachments=['/path/on/hdfs.txt',
                     '/another/path/on/hdfs.txt'],
    ).xml(indent=True)
    assert tests.utils.xml_to_comparable_dict('''
    <email xmlns="uri:oozie:email-action:0.2">
        <to>b.a.baracus@example.com,mrt@example.com</to>
        <subject>Chains</subject>
        <body>Do you need more?</body>
        <cc>ateam@example.com,webmaster@example.com</cc>
        <bcc>goldchains4u@example.com,jewelrystore@example.com</bcc>
        <content_type>text/plain</content_type>
        <attachment>/another/path/on/hdfs.txt,/path/on/hdfs.txt</attachment>
    </email>
    ''') == tests.utils.xml_to_comparable_dict(actual)


def parse_datetime(string):
    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%MZ')


def test_minimal_coordinator(minimal_coordinator_options):
    actual_xml = tags.CoordinatorApp(**minimal_coordinator_options).xml(indent=True)
    actual_dict = tests.utils.xml_to_comparable_dict(actual_xml)

    expected_xml = None
    with open('tests/data/minimal_coordinator.xml', 'r') as fh:
        expected_xml = fh.read()
    expected_dict = tests.utils.xml_to_comparable_dict(expected_xml)

    assert actual_dict == expected_dict


def test_full_coordinator(minimal_coordinator_options):
    full_coordinator_options = minimal_coordinator_options
    full_coordinator_options.update({
        'end': parse_datetime('2115-01-01T10:56Z'),
        'concurrency': 1,
        'throttle': '${throttle}',
        'timeout': 10,
        'execution_order': tags.EXEC_LAST_ONLY,
        'parameters': {
            'throttle': 1,
        },
        'workflow_configuration': {
            'mapred.job.queue.name': 'production',
        },
    })

    actual_xml = tags.CoordinatorApp(**minimal_coordinator_options).xml(indent=True)
    actual_dict = tests.utils.xml_to_comparable_dict(actual_xml)

    expected_xml = None
    with open('tests/data/full_coordinator.xml', 'r') as fh:
        expected_xml = fh.read()
    expected_dict = tests.utils.xml_to_comparable_dict(expected_xml)

    assert actual_dict == expected_dict


def test_really_long_coordinator_name(minimal_coordinator_options):
    with pytest.raises(AssertionError) as assertion_info:
        del minimal_coordinator_options['name']
        tags.CoordinatorApp(name='l' * (tags.MAX_NAME_LENGTH + 1), **minimal_coordinator_options)
    assert "Name must be less than" in str(assertion_info.value)


def test_coordinator_bad_frequency(minimal_coordinator_options):
    minimal_coordinator_options['frequency'] = 0
    with pytest.raises(AssertionError) as assertion_info:
        tags.CoordinatorApp(**minimal_coordinator_options)
    assert str(assertion_info.value) == \
        'Frequency (0 min) must be greater than or equal to 5 min'


def test_coordinator_end_before_start(minimal_coordinator_options):
    minimal_coordinator_options['end'] = minimal_coordinator_options['start'] - datetime.timedelta(days=10)
    with pytest.raises(AssertionError) as assertion_info:
        tags.CoordinatorApp(**minimal_coordinator_options)
    assert str(assertion_info.value) == \
        'End time (2014-12-22T10:56Z) must be greater than the start time (2015-01-01T10:56Z)'


def assert_workflow(request, workflow_app, expected_xml=None):
    # What does this doc look like?
    actual_xml = workflow_app.xml(indent=True)
    if request.config.getoption('verbose') > 2:
        print(actual_xml.decode('utf-8'))

    # Is this a valid XML doc?
    tests.utils.assert_valid_workflow(actual_xml)

    # Is this the doc that we expect?
    if expected_xml:
        actual_dict = tests.utils.xml_to_comparable_dict(actual_xml)
        expected_dict = tests.utils.xml_to_comparable_dict(expected_xml)
        assert expected_dict == actual_dict


def test_workflow_app(request):
    workflow_app = tags.WorkflowApp(
        name='descriptive-name',
        parameters={'property_key': 'property_value'},
        configuration={'config_key': 'config_value'},
        credentials=[tags.Credential(
            {'cred_name': 'cred_value'},
            credential_name='my-hcat-creds',
            credential_type='hcat')],
        job_tracker='job-tracker',
        name_node='name-node',
        job_xml_files=['/user/${wf:user()}/job.xml'],
    )
    assert_workflow(request, workflow_app, """
<workflow-app xmlns="uri:oozie:workflow:0.5" name="descriptive-name">
    <parameters>
        <property>
            <name>property_key</name>
            <value>property_value</value>
        </property>
    </parameters>
    <global>
        <job-tracker>job-tracker</job-tracker>
        <name-node>name-node</name-node>
        <job-xml>/user/${wf:user()}/job.xml</job-xml>
        <configuration>
            <property>
                <name>config_key</name>
                <value>config_value</value>
            </property>
        </configuration>
    </global>
    <credentials>
        <credential type="hcat" name="my-hcat-creds">
            <property>
                <name>cred_name</name>
                <value>cred_value</value>
            </property>
        </credential>
    </credentials>
    <start to="end" />
    <end name="end" />
</workflow-app>
""")


def test_workflow_action(request):
    entities = tags.Action(
        name='action-name',
        action=tags.Shell(exec_command='echo', arguments=['build']),
    )
    assert len(set(entities)) == 1
    assert bool(entities)

    workflow_app = tags.WorkflowApp(
        name='descriptive-name',
        job_tracker='job-tracker',
        name_node='name-node',
        entities=entities
    )
    assert_workflow(request, workflow_app, """
<workflow-app xmlns="uri:oozie:workflow:0.5" name="descriptive-name">
    <global>
        <job-tracker>job-tracker</job-tracker>
        <name-node>name-node</name-node>
    </global>
    <start to="action-action-name" />
    <action name="action-action-name">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>echo</exec>
            <argument>build</argument>
        </shell>
        <ok to="end" />
        <error to="end" />
    </action>
    <end name="end" />
</workflow-app>
""")

    entities = tags.Action(
        name='action-name',
        action=tags.Shell(exec_command='echo', arguments=['build']),
        credential='my-hcat-creds',
        retry_max=10,
        retry_interval=20,
        on_error=tags.Kill(name='error', message='A bad thing happened'),
    )
    assert len(set(entities)) == 2
    assert bool(entities)

    workflow_app = tags.WorkflowApp(
        name='descriptive-name',
        job_tracker='job-tracker',
        name_node='name-node',
        credentials=[tags.Credential(
            {'cred_name': 'cred_value'},
            credential_name='my-hcat-creds',
            credential_type='hcat')],
        entities=entities
    )
    assert_workflow(request, workflow_app, """
<workflow-app xmlns="uri:oozie:workflow:0.5" name="descriptive-name">
    <global>
        <job-tracker>job-tracker</job-tracker>
        <name-node>name-node</name-node>
    </global>
    <credentials>
        <credential type="hcat" name="my-hcat-creds">
            <property>
                <name>cred_name</name>
                <value>cred_value</value>
            </property>
        </credential>
    </credentials>
    <start to="action-action-name" />
    <action retry-max="10" cred="my-hcat-creds" name="action-action-name" retry-interval="20">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>echo</exec>
            <argument>build</argument>
        </shell>
        <ok to="end" />
        <error to="kill-error" />
    </action>
    <kill name="kill-error">
        <message>A bad thing happened</message>
    </kill>
    <end name="end" />
</workflow-app>
""")


def test_workflow_action_retries(request):
    def workflow_app_xml(**kwargs):
        workflow_app = tags.WorkflowApp(
            name='descriptive-name',
            job_tracker='job-tracker',
            name_node='name-node',
            entities=tags.Action(
                name='name',
                action=tags.Shell(exec_command='echo', arguments=['build']),
                **kwargs
            )
        )
        assert_workflow(request, workflow_app)
        return tests.utils.ParsedXml(workflow_app.xml())

    # Test that no retry is specified (default is used)
    workflow_app_xml().assert_node('/action', name='action-name')

    # Test that 0 can be set for retry parameters
    workflow_app_xml(retry_max=0, retry_interval=0).assert_node(
        '/action', **{'name': 'action-name', 'retry-max': '0', 'retry-interval': '0'})

    # Test that no retry is specified (default is used)
    workflow_app_xml(retry_max=1, retry_interval=2).assert_node(
        '/action', **{'name': 'action-name', 'retry-max': '1', 'retry-interval': '2'})


def test_workflow_action_without_credential():
    with pytest.raises(AssertionError) as assertion_info:
        tags.WorkflowApp(
            name='descriptive-name',
            job_tracker='job-tracker',
            name_node='name-node',
            entities=tags.Action(
                name='action-name',
                action=tags.Shell(exec_command='echo', arguments=['build']),
                credential='my-hcat-creds',
                retry_max=10,
                retry_interval=20,
            )
        )
    assert str(assertion_info.value) == str('Missing credentials: my-hcat-creds')


def test_workflow_with_reused_identifier():
    with pytest.raises(AssertionError) as assertion_info:
        tags.WorkflowApp(
            name='descriptive-name',
            job_tracker='job-tracker',
            name_node='name-node',
            entities=tags.Action(
                name='build', action=tags.Shell(exec_command='echo', arguments=['build']),
                on_error=tags.Action(name='build', action=tags.Shell(exec_command='echo', arguments=['error']))
            )
        )
    assert str(assertion_info.value) == 'Name(s) reused: action-build'

    with pytest.raises(AssertionError) as assertion_info:
        tags.WorkflowApp(
            name='descriptive-name',
            job_tracker='job-tracker',
            name_node='name-node',
            actions=tags.Serial(
                tags.Action(name='build', action=tags.Shell(exec_command='echo', arguments=['build'])),
                tags.Action(name='resolve', action=tags.Shell(exec_command='echo', arguments=['resolve'])),
                on_error=tags.Serial(
                    tags.Action(name='build', action=tags.Shell(exec_command='echo', arguments=['error'])),
                    tags.Kill('A bad thing happened')
                )
            )
        )
    assert str(assertion_info.value) == 'Name(s) reused: action-build'


def test_workflow_app_empty_serial_actions():
    # Empty collections should act empty
    assert len(set(tags.Serial())) == 0
    assert not bool(tags.Serial())


def test_workflow_app_serial_actions(request):
    # Create a serial collection of actions with a serial collection as an error condition
    actions = tags.Serial(
        tags.Action(tags.Shell(exec_command='echo', arguments=['build'])),
        tags.Action(tags.Shell(exec_command='echo', arguments=['resolve'])),
        on_error=tags.Serial(
            tags.Action(tags.Shell(exec_command='echo', arguments=['error'])),
            tags.Kill('A bad thing happened')
        )
    )
    assert len(set(actions)) == 4
    assert bool(actions)

    workflow_app = tags.WorkflowApp(
        name='descriptive-name',
        job_tracker='job-tracker',
        name_node='name-node',
        actions=actions
    )
    assert_workflow(request, workflow_app, """
<workflow-app xmlns="uri:oozie:workflow:0.5" name="descriptive-name">
    <global>
        <job-tracker>job-tracker</job-tracker>
        <name-node>name-node</name-node>
    </global>
    <start to="action-00000000" />
    <action name="action-00000002">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>echo</exec>
            <argument>error</argument>
        </shell>
        <ok to="kill-00000003" />
        <error to="end" />
    </action>
    <kill name="kill-00000003">
        <message>A bad thing happened</message>
    </kill>
    <action name="action-00000000">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>echo</exec>
            <argument>build</argument>
        </shell>
        <ok to="action-00000001" />
        <error to="action-00000002" />
    </action>
    <action name="action-00000001">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>echo</exec>
            <argument>resolve</argument>
        </shell>
        <ok to="end" />
        <error to="action-00000002" />
    </action>
    <end name="end" />
</workflow-app>
""")


def test_workflow_app_empty_parallel_actions():
    # Cannot create an empty parallel collection
    with pytest.raises(AssertionError) as assertion_info:
        tags.Parallel()
    assert str(assertion_info.value) == 'At least 1 action required'


def test_workflow_app_parallel_actions(request):
    # Create a simple parallel collection
    actions = tags.Parallel(
        tags.Action(tags.Shell(exec_command='echo', arguments=['build'])),
        tags.Action(tags.Shell(exec_command='echo', arguments=['resolve'])),
        on_error=tags.Serial(
            tags.Action(tags.Shell(exec_command='echo', arguments=['error'])),
            tags.Kill('A bad thing happened')
        )
    )
    assert len(set(actions)) == 4
    assert bool(actions)

    workflow_app = tags.WorkflowApp(
        name='descriptive-name',
        job_tracker='job-tracker',
        name_node='name-node',
        actions=actions
    )
    assert_workflow(request, workflow_app, """
<workflow-app xmlns="uri:oozie:workflow:0.5" name="descriptive-name">
    <global>
        <job-tracker>job-tracker</job-tracker>
        <name-node>name-node</name-node>
    </global>
    <start to="fork-00000005" />
    <action name="action-00000002">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>echo</exec>
            <argument>error</argument>
        </shell>
        <ok to="kill-00000003" />
        <error to="end" />
    </action>
    <kill name="kill-00000003">
        <message>A bad thing happened</message>
    </kill>
    <fork name="fork-00000005">
        <path start="action-00000000" />
        <path start="action-00000001" />
    </fork>
    <action name="action-00000000">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>echo</exec>
            <argument>build</argument>
        </shell>
        <ok to="join-00000005" />
        <error to="action-00000002" />
    </action>
    <action name="action-00000001">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>echo</exec>
            <argument>resolve</argument>
        </shell>
        <ok to="join-00000005" />
        <error to="action-00000002" />
    </action>
    <join name="join-00000005" to="end" />
    <end name="end" />
</workflow-app>
""")


def test_workflow_app_inherit_parent_error(request):
    workflow_app = tags.WorkflowApp(
        name='descriptive-name',
        job_tracker='job-tracker',
        name_node='name-node',
        actions=tags.Serial(
            tags.Parallel(
                tags.Action(name='build_a', action=tags.Shell(exec_command='echo', arguments=['build_a'])),
                tags.Action(name='build_b', action=tags.Shell(exec_command='echo', arguments=['build_b'])),
                name='builders'
            ),
            tags.Parallel(
                tags.Action(name='resolve_a', action=tags.Shell(exec_command='echo', arguments=['resolve_a'])),
                tags.Action(name='resolve_b', action=tags.Shell(exec_command='echo', arguments=['resolve_b'])),
                name='resolvers'
            ),
            on_error=tags.Serial(
                tags.Action(name='error', action=tags.Shell(exec_command='echo', arguments=['error'])),
                tags.Kill(name='error', message='A bad thing happened')
            )
        )
    )

    assert_workflow(request, workflow_app)

    app = tests.utils.ParsedXml(workflow_app.xml())
    app.assert_node("/action[@name='action-build_a']/error", to='action-error')
    app.assert_node("/action[@name='action-build_b']/error", to='action-error')
    app.assert_node("/action[@name='action-resolve_a']/error", to='action-error')
    app.assert_node("/action[@name='action-resolve_a']/error", to='action-error')
    app.assert_node("/action[@name='action-error']/error", to='end')
    app.assert_node("/action[@name='action-error']/ok", to='kill-error')
