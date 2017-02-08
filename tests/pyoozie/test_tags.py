# -*- coding: utf-8 -*-
# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a BSD-style license that can be found in the LICENSE file.
from __future__ import unicode_literals, print_function

import decimal
import pytest

from pyoozie import Parameters, Configuration, Credentials, Shell, SubWorkflow, GlobalConfiguration, Email
from pyoozie.tags import _validate
from tests.utils import xml_to_dict_unordered


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


def test_validate():
    _validate('ok-id')

    _validate('very-long-flow-name-that-spans-39-chars')

    with pytest.raises(AssertionError) as assertion_info:
        _validate('too-long-flow-name-that-spans-more-than-39-chars')
    assert str(assertion_info.value) == "Identifier must be less than 39 " \
        "chars long, 'too-long-flow-name-that-spans-more-than-39-chars' is 48"

    with pytest.raises(AssertionError) as assertion_info:
        _validate('0-id-starting-with-a-non-alpha-char')
    assert str(assertion_info.value) == "Identifier must match ^[a-zA-Z_]" \
        "[\\-_a-zA-Z0-9]{0,38}$, '0-id-starting-with-a-non-alpha-char' " \
        "does not"

    with pytest.raises(AssertionError) as assertion_info:
        _validate('id.with.illlegal.chars')
    assert str(assertion_info.value) == "Identifier must match ^[a-zA-Z_]" \
        "[\\-_a-zA-Z0-9]{0,38}$, 'id.with.illlegal.chars' does not"


def test_parameters(expected_property_values, expected_property_values_xml):
    actual = Parameters(expected_property_values).xml(indent=True)
    expected = '''<parameters>%s</parameters>''' % expected_property_values_xml
    assert xml_to_dict_unordered(expected) == xml_to_dict_unordered(actual)


def test_configuration(expected_property_values, expected_property_values_xml):
    actual = Configuration(expected_property_values).xml(indent=True)
    expected = '''<configuration>%s</configuration>''' % \
        expected_property_values_xml
    assert xml_to_dict_unordered(expected) == xml_to_dict_unordered(actual)


def test_credentials(expected_property_values, expected_property_values_xml):
    actual = Credentials(expected_property_values,
                         credential_name='my-hcat-creds',
                         credential_type='hcat').xml(indent=True)
    expected = '''
        <credentials name='my-hcat-creds' type='hcat'>%s</credentials>
        ''' % expected_property_values_xml
    assert xml_to_dict_unordered(expected) == xml_to_dict_unordered(actual)


def test_shell():
    actual = Shell(
        exec_command='${EXEC}',
        job_tracker='${jobTracker}',
        name_node='${nameNode}',
        prepares=None,
        job_xml_files=['/user/${wf:user()}/job.xml'],
        configuration={
            'mapred.job.queue.name': '${queueName}'
        },
        arguments=['A', 'B'],
        env_vars=None,
        files=['/users/blabla/testfile.sh#testfile'],
        archives=['/users/blabla/testarchive.jar#testarchive'],
        capture_output=False
    ).xml(indent=True)
    assert xml_to_dict_unordered('''
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
    </shell>''') == xml_to_dict_unordered(actual)


def test_subworkflow():
    app_path = '/user/username/workflows/cool-flow'

    actual = SubWorkflow(
        app_path=app_path,
        propagate_configuration=False,
        configuration=None,
    ).xml(indent=True)
    assert xml_to_dict_unordered('''
    <sub-workflow>
        <app-path>/user/username/workflows/cool-flow</app-path>
    </sub-workflow>
    ''') == xml_to_dict_unordered(actual)

    actual = SubWorkflow(
        app_path=app_path,
        propagate_configuration=True,
        configuration=None,
    ).xml(indent=True)
    assert xml_to_dict_unordered('''
    <sub-workflow>
        <app-path>/user/username/workflows/cool-flow</app-path>
        <propagate-configuration />
    </sub-workflow>
    ''') == xml_to_dict_unordered(actual)

    actual = SubWorkflow(
        app_path=app_path,
        propagate_configuration=True,
        configuration={
            'job_tracker': 'a_jobtracker',
            'name_node': 'hdfs://localhost:50070',
        },
    ).xml(indent=True)
    assert xml_to_dict_unordered('''
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
    ''') == xml_to_dict_unordered(actual)


def test_global_configuration():
    configuration = {
        'mapred.job.queue.name': '${queueName}'
    }

    actual = GlobalConfiguration(
        job_tracker='a_jobtracker',
        name_node='hdfs://localhost:50070',
        job_xml_files=None,
        configuration=None,
    ).xml(indent=True)
    assert xml_to_dict_unordered('''
    <global>
        <job-tracker>a_jobtracker</job-tracker>
        <name-node>hdfs://localhost:50070</name-node>
    </global>
    ''') == xml_to_dict_unordered(actual)

    actual = GlobalConfiguration(
        job_tracker='a_jobtracker',
        name_node='hdfs://localhost:50070',
        job_xml_files=['/user/${wf:user()}/job.xml'],
        configuration=configuration,
    ).xml(indent=True)
    assert xml_to_dict_unordered('''
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
    ''') == xml_to_dict_unordered(actual)


def test_email():
    actual = Email(
        to='mrt@theateam.com',
        subject='Chains',
        body='Do you need more?',
    ).xml(indent=True)
    assert xml_to_dict_unordered('''
    <email xmlns="uri:oozie:email-action:0.2">
        <to>mrt@theateam.com</to>
        <subject>Chains</subject>
        <body>Do you need more?</body>
    </email>
    ''') == xml_to_dict_unordered(actual)

    actual = Email(
        to='mrt@theateam.com',
        subject='Chains',
        body='Do you need more?',
        cc='ateam@ateam.com',
        bcc='jewelrystore@myshopify.com',
        content_type='text/plain',
        attachments='/path/to/attachment/on/hdfs.txt',
    ).xml(indent=True)
    assert xml_to_dict_unordered('''
    <email xmlns="uri:oozie:email-action:0.2">
        <to>mrt@theateam.com</to>
        <subject>Chains</subject>
        <body>Do you need more?</body>
        <cc>ateam@ateam.com</cc>
        <bcc>jewelrystore@myshopify.com</bcc>
        <content_type>text/plain</content_type>
        <attachment>/path/to/attachment/on/hdfs.txt</attachment>
    </email>
    ''') == xml_to_dict_unordered(actual)

    actual = Email(
        to=['mrt@theateam.com', 'b.a.baracus@theateam.com'],
        subject='Chains',
        body='Do you need more?',
        cc=('ateam@ateam.com', 'webmaster@theateam.com'),
        bcc=set(['jewelrystore@myshopify.com', 'goldchains4u@myshopify.com']),
        content_type='text/plain',
        attachments=['/path/on/hdfs.txt',
                     '/another/path/on/hdfs.txt'],
    ).xml(indent=True)
    assert xml_to_dict_unordered('''
    <email xmlns="uri:oozie:email-action:0.2">
        <to>b.a.baracus@theateam.com,mrt@theateam.com</to>
        <subject>Chains</subject>
        <body>Do you need more?</body>
        <cc>ateam@ateam.com,webmaster@theateam.com</cc>
        <bcc>goldchains4u@myshopify.com,jewelrystore@myshopify.com</bcc>
        <content_type>text/plain</content_type>
        <attachment>/another/path/on/hdfs.txt,/path/on/hdfs.txt</attachment>
    </email>
    ''') == xml_to_dict_unordered(actual)
