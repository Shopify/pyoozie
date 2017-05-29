# -*- coding: utf-8 -*-
# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import xml.etree.ElementTree as et

import pytest
import six
import tests.utils


@pytest.fixture
def valid_workflow():
    return """
<workflow-app xmlns="uri:oozie:workflow:0.5" name="descriptive-name">
    <parameters>
        <property>
            <name>property_key</name>
            <value>property_value</value>
        </property>
        <property>
            <name>other_property_key</name>
            <value>🤣</value>
        </property>
    </parameters>
    <global>
        <job-tracker>job-tracker</job-tracker>
        <name-node>name-node-🙄</name-node>
    </global>
    <start to="end" />
    <end name="end" />
</workflow-app>
""".strip()


def test_xml_to_dict_unordered():
    document_dict = tests.utils.xml_to_dict_unordered("""
<root>
  <tag />
  <tag key="value" />
  <tag>Text</tag>
  <tag key="value">Text</tag>
  <tag different-key="different-value"> Text </tag>
</root>
    """.strip())
    assert document_dict == {
        'root': {
            'tag': (
                None,
                {'@key': 'value'},
                'Text',
                {'#text': 'Text', '@key': 'value'},
                {'#text': 'Text', '@different-key': 'different-value'}
            )
        }
    }


def test_assert_valid_workflow(valid_workflow):
    # A minimal workflow should validate
    tests.utils.assert_valid_workflow(valid_workflow)

    # With valid XML, duplicate names should raise an assertion error
    with pytest.raises(AssertionError) as assertion_info:
        xml = """
<workflow-app xmlns="uri:oozie:workflow:0.5" name="descriptive-name">
    <action name="alpha"/>
    <action name="alpha"/>
    <action name="beta"/>
</workflow-app>""".strip()
        assert len(et.fromstring(xml)), 'Invalid test XML'
        tests.utils.assert_valid_workflow(xml)
    assert str(assertion_info.value) == 'Name(s) reused: alpha'

    # With valid XML, a missing start tag should result in a schema violation assertion error
    with pytest.raises(AssertionError) as assertion_info:
        xml = """
<workflow-app xmlns="uri:oozie:workflow:0.5" name="descriptive-name">
    <global>
        <job-tracker>job-tracker</job-tracker>
        <name-node>name-node</name-node>
    </global>
    <end name="end" />
</workflow-app>""".strip()
        assert len(et.fromstring(xml)), 'Invalid test XML'
        tests.utils.assert_valid_workflow(xml)
    assert str(assertion_info.value).strip() == str(
        '''An XML validation error occurred.

Error: Invalid app definition, org.xml.sax.SAXParseException; lineNumber: 6; columnNumber: 23; '''
        '''cvc-complex-type.2.4.a: Invalid content was found starting with element 'end'. One of '''
        ''''{"uri:oozie:workflow:0.5":credentials, "uri:oozie:workflow:0.5":start}' is expected.

Parsing:

''' + xml).strip()

    # With invalid XML, a parsing error should occur
    with pytest.raises(et.ParseError) as assertion_info:
        xml = '<workflow-app xmlns="uri:oozie:workflow:0.5" name="descriptive-name">'
        tests.utils.assert_valid_workflow(xml)
    assert str(assertion_info.value) == 'no element found: line 1, column 38'


def test_parsed_xml_assert_node(valid_workflow):
    app = tests.utils.ParsedXml(valid_workflow)

    # Asserting that a node that exists (once) should pass
    app.assert_node('/global')

    # Asserting that a node that doesn't exists should raise an assertion error
    with pytest.raises(AssertionError) as assertion_info:
        app.assert_node('/not_a_tag')
    assert str(assertion_info.value) == str("Could not find xml tag at xpath '/not_a_tag'")

    # Asserting that a node that exists (when it does multiple times) should raise an assertion error
    with pytest.raises(AssertionError) as assertion_info:
        app.assert_node('/parameters/property')
    assert str(assertion_info.value) == str("Found more than one resolution of xpath '/parameters/property'")

    app.assert_node('/global/name-node', 'name-node-🙄')
    
    # Asserting that a node has a specific text value when it doesn't should raise an error
    with pytest.raises(AssertionError) as assertion_info:
        app.assert_node('/global/name-node', 'name-node')
    assert six.text_type(assertion_info.value) == 'name-node-🙄 != name-node'
    
    # Asserting that a node has a specific attribute should pass
    app.assert_node('/start', to='end')

    # Asserting that a node has a specific attribute value that it doesn't should raise an assertion error
    with pytest.raises(AssertionError) as assertion_info:
        app.assert_node('/start', to=str('not_end'))
    assert str(assertion_info.value) == str("{'to': 'not_end'} != {'to': 'end'}")

    # Asserting that a node has a specific attribute that it's missing should raise an assertion error
    with pytest.raises(AssertionError) as assertion_info:
        app.assert_node('/start', not_a_key=str('end'))
    assert str(assertion_info.value) == str("{'not_a_key': 'end'} != {'to': 'end'}")
