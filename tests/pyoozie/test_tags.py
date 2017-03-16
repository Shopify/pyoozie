# -*- coding: utf-8 -*-
# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import datetime
import decimal

import pytest
import six
import tests.utils

from pyoozie import tags


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
    assert tests.utils.xml_to_dict_unordered(expected) == tests.utils.xml_to_dict_unordered(actual)


def test_configuration(expected_property_values, expected_property_values_xml):
    actual = tags.Configuration(expected_property_values).xml(indent=True)
    expected = '<configuration>{xml}</configuration>'.format(xml=expected_property_values_xml)
    assert tests.utils.xml_to_dict_unordered(expected) == tests.utils.xml_to_dict_unordered(actual)


def test_credential(expected_property_values, expected_property_values_xml):
    actual = tags.Credential(expected_property_values,
                             credential_name='my-hcat-creds',
                             credential_type='hcat').xml(indent=True)
    expected = "<credential name='my-hcat-creds' type='hcat'>{xml}</credential>".format(
        xml=expected_property_values_xml)
    assert tests.utils.xml_to_dict_unordered(expected) == tests.utils.xml_to_dict_unordered(actual)


def test_shell():
    actual = tags.Shell(
        exec_command='${EXEC}',
        job_tracker='${jobTracker}',
        name_node='${nameNode}',
        prepares=None,
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
    assert tests.utils.xml_to_dict_unordered('''
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
    </shell>''') == tests.utils.xml_to_dict_unordered(actual)

    # Test using prepares fails
    with pytest.raises(NotImplementedError) as assertion_info:
        tags.Shell(
            exec_command='${EXEC}',
            prepares=['anything'],
        ).xml()
    assert str(assertion_info.value) == "Shell action's prepares has not yet been implemented"


def test_subworkflow():
    app_path = '/user/username/workflows/cool-flow'

    actual = tags.SubWorkflow(
        app_path=app_path,
        propagate_configuration=False,
        configuration=None,
    ).xml(indent=True)
    assert tests.utils.xml_to_dict_unordered('''
    <sub-workflow>
        <app-path>/user/username/workflows/cool-flow</app-path>
    </sub-workflow>
    ''') == tests.utils.xml_to_dict_unordered(actual)

    actual = tags.SubWorkflow(
        app_path=app_path,
        propagate_configuration=True,
        configuration=None,
    ).xml(indent=True)
    assert tests.utils.xml_to_dict_unordered('''
    <sub-workflow>
        <app-path>/user/username/workflows/cool-flow</app-path>
        <propagate-configuration />
    </sub-workflow>
    ''') == tests.utils.xml_to_dict_unordered(actual)

    actual = tags.SubWorkflow(
        app_path=app_path,
        propagate_configuration=True,
        configuration={
            'job_tracker': 'a_jobtracker',
            'name_node': 'hdfs://localhost:50070',
        },
    ).xml(indent=True)
    assert tests.utils.xml_to_dict_unordered('''
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
    ''') == tests.utils.xml_to_dict_unordered(actual)


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
    assert tests.utils.xml_to_dict_unordered('''
    <global>
        <job-tracker>a_jobtracker</job-tracker>
        <name-node>hdfs://localhost:50070</name-node>
    </global>
    ''') == tests.utils.xml_to_dict_unordered(actual)

    actual = tags.GlobalConfiguration(
        job_tracker='a_jobtracker',
        name_node='hdfs://localhost:50070',
        job_xml_files=['/user/${wf:user()}/job.xml'],
        configuration=configuration,
    ).xml(indent=True)
    assert tests.utils.xml_to_dict_unordered('''
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
    ''') == tests.utils.xml_to_dict_unordered(actual)


def test_email():
    actual = tags.Email(
        to='mrt@example.com',
        subject='Chains',
        body='Do you need more?',
    ).xml(indent=True)
    assert tests.utils.xml_to_dict_unordered('''
    <email xmlns="uri:oozie:email-action:0.2">
        <to>mrt@example.com</to>
        <subject>Chains</subject>
        <body>Do you need more?</body>
    </email>
    ''') == tests.utils.xml_to_dict_unordered(actual)

    actual = tags.Email(
        to='mrt@example.com',
        subject='Chains',
        body='Do you need more?',
        cc='ateam@example.com',
        bcc='jewelrystore@example.com',
        content_type='text/plain',
        attachments='/path/to/attachment/on/hdfs.txt',
    ).xml(indent=True)
    assert tests.utils.xml_to_dict_unordered('''
    <email xmlns="uri:oozie:email-action:0.2">
        <to>mrt@example.com</to>
        <subject>Chains</subject>
        <body>Do you need more?</body>
        <cc>ateam@example.com</cc>
        <bcc>jewelrystore@example.com</bcc>
        <content_type>text/plain</content_type>
        <attachment>/path/to/attachment/on/hdfs.txt</attachment>
    </email>
    ''') == tests.utils.xml_to_dict_unordered(actual)

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
    assert tests.utils.xml_to_dict_unordered('''
    <email xmlns="uri:oozie:email-action:0.2">
        <to>b.a.baracus@example.com,mrt@example.com</to>
        <subject>Chains</subject>
        <body>Do you need more?</body>
        <cc>ateam@example.com,webmaster@example.com</cc>
        <bcc>goldchains4u@example.com,jewelrystore@example.com</bcc>
        <content_type>text/plain</content_type>
        <attachment>/another/path/on/hdfs.txt,/path/on/hdfs.txt</attachment>
    </email>
    ''') == tests.utils.xml_to_dict_unordered(actual)


def parse_datetime(string):
    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%MZ')


def test_coordinator(coordinator_xml, expected_coordinator_options):
    actual = tags.Coordinator(**expected_coordinator_options).xml()
    assert tests.utils.xml_to_dict_unordered(coordinator_xml) == tests.utils.xml_to_dict_unordered(actual)


def test_coordinator_end_default(coordinator_xml, expected_coordinator_options):
    del expected_coordinator_options['end']
    actual = tags.Coordinator(**expected_coordinator_options).xml()
    assert tests.utils.xml_to_dict_unordered(coordinator_xml) == tests.utils.xml_to_dict_unordered(actual)


def test_coordinator_with_controls_and_more(coordinator_xml_with_controls, expected_coordinator_options):
    actual = tags.Coordinator(
        timeout=10,
        concurrency=1,
        execution_order=tags.EXEC_LAST_ONLY,
        throttle='${throttle}',
        workflow_configuration=tags.Configuration({
            'mapred.job.queue.name': 'production'
        }),
        parameters=tags.Parameters({
            'throttle': 1
        }),
        **expected_coordinator_options
    ).xml()
    expected_dict = tests.utils.xml_to_dict_unordered(coordinator_xml_with_controls)
    actual_dict = tests.utils.xml_to_dict_unordered(actual)
    assert expected_dict == actual_dict


def test_really_long_coordinator_name(expected_coordinator_options):
    with pytest.raises(AssertionError) as assertion_info:
        del expected_coordinator_options['name']
        tags.Coordinator(name='l' * (tags.MAX_NAME_LENGTH + 1), **expected_coordinator_options)
    assert "Name must be less than" in str(assertion_info.value)


def test_coordinator_bad_frequency(expected_coordinator_options):
    expected_coordinator_options['frequency'] = 0
    with pytest.raises(AssertionError) as assertion_info:
        tags.Coordinator(**expected_coordinator_options)
    assert str(assertion_info.value) == \
        'Frequency (0 min) must be greater than or equal to 5 min'


def test_coordinator_end_before_start(expected_coordinator_options):
    expected_coordinator_options['end'] = expected_coordinator_options['start'] - datetime.timedelta(days=10)
    with pytest.raises(AssertionError) as assertion_info:
        tags.Coordinator(**expected_coordinator_options)
    assert str(assertion_info.value) == \
        'End time (2014-12-22T10:56Z) must be greater than the start time (2015-01-01T10:56Z)'
