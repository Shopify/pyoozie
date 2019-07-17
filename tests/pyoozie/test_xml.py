# -*- coding: utf-8 -*-
# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

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


def test_workflow_submission_xml(username, workflow_app_path):
    actual = xml._workflow_submission_xml(
        username=username,
        workflow_xml_path=workflow_app_path,
        indent=True,
    )
    assert tests.utils.xml_to_comparable_dict('''
    <configuration>
        <property>
            <name>oozie.wf.application.path</name>
            <value>/user/oozie/workflows/descriptive-name</value>
        </property>
        <property>
            <name>user.name</name>
            <value>test</value>
        </property>
    </configuration>''') == tests.utils.xml_to_comparable_dict(actual)


def test_workflow_submission_xml_with_configuration(username, workflow_app_path):
    actual = xml._workflow_submission_xml(
        username=username,
        workflow_xml_path=workflow_app_path,
        configuration={
            'other.key': 'other value',
        },
        indent=True
    )

    assert tests.utils.xml_to_comparable_dict('''
    <configuration>
        <property>
            <name>oozie.wf.application.path</name>
            <value>/user/oozie/workflows/descriptive-name</value>
        </property>
        <property>
            <name>other.key</name>
            <value>other value</value>
        </property>
        <property>
            <name>user.name</name>
            <value>test</value>
        </property>
    </configuration>''') == tests.utils.xml_to_comparable_dict(actual)


def test_coordinator_submission_xml(username, coord_app_path):
    actual = xml._coordinator_submission_xml(
        username=username,
        coord_xml_path=coord_app_path,
        indent=True
    )
    assert tests.utils.xml_to_comparable_dict('''
    <configuration>
        <property>
            <name>oozie.coord.application.path</name>
            <value>/user/oozie/coordinators/descriptive-name</value>
        </property>
        <property>
            <name>user.name</name>
            <value>test</value>
        </property>
    </configuration>''') == tests.utils.xml_to_comparable_dict(actual)


def test_coordinator_submission_xml_with_configuration(username, coord_app_path):
    actual = xml._coordinator_submission_xml(
        username=username,
        coord_xml_path=coord_app_path,
        configuration={
            'oozie.coord.group.name': 'descriptive-group',
        },
        indent=True
    )
    assert tests.utils.xml_to_comparable_dict('''
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
    </configuration>''') == tests.utils.xml_to_comparable_dict(actual)
