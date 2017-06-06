# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import collections
import re
import subprocess
import tempfile
import xml.etree.ElementTree as et

import six
import xmltodict


def xml_to_comparable_dict(xml):
    
    def _sort_key(value):
        """Recursively sort lists embedded within dicts."""
        if hasattr(value, 'items'):
            return repr(sorted([(k, _sort_key(v)) for k, v in value.items()]))
        elif isinstance(value, (tuple, set, list)):
            return repr(sorted(value, key=_sort_key))
        else:
            return repr(value)
        
    def _unorder(value):
        """Convert from a `collections.OrderedDict` to a `dict` with predictably sorted lists."""
        if hasattr(value, 'items'):
            return {k: _unorder(v) for k, v in value.items()}
        elif isinstance(value, (tuple, set, list)):
            return sorted(tuple(_unorder(v) for v in value), key=_sort_key)
        else:
            return value
    return _unorder(xmltodict.parse(xml))


NAMESPACE_ATTRIBUTE = re.compile(r' xmlns:?[a-z0-9]*="[^"]+"', flags=re.UNICODE)


class ParsedXml(object):
    """Parses an XML string and provides methods to make assertions about the resulting element tree."""

    def __init__(self, xml_string):
        if isinstance(xml_string, six.binary_type):
            xml_string = xml_string.decode('utf-8')
        xml_string = NAMESPACE_ATTRIBUTE.sub(str(''), xml_string.strip())
        self.tree = et.ElementTree(et.fromstring(xml_string.encode('utf-8')))

    def __get_elements(self, xpath):
        elements = self.tree.findall(xpath)
        assert elements, "Could not find xml tag at xpath '%s'" % xpath
        return elements

    def __get_element(self, xpath):
        elements = self.__get_elements(xpath)
        assert len(elements) == 1, \
            "Found more than one resolution of xpath '%s'" % xpath
        return elements[0]

    def assert_node(self, xpath, *args, **kwargs):
        element = self.__get_element(xpath)
        if kwargs:
            assert set(kwargs.items()) <= set(element.attrib.items()), (
                ('%r != %r' % (kwargs, element.attrib))
                .replace(": u'", ": '"))
        if args:
            assert len(args) == 1, 'Too many positional arguments specified'
            assert element.text == args[0], '%s != %s' % (element.text, args[0])


def assert_valid_workflow(xml):
    # Check for duplicate names (valid XML and valid schema, but logically invalid)
    names = ParsedXml(xml).tree.findall('.//*[@name]')
    names = [name.attrib['name'] for name in names]
    duplicate_names = [item for item, count in collections.Counter(names).items() if count > 1]
    assert not duplicate_names, 'Name(s) reused: %s' % ', '.join(sorted(duplicate_names))

    if isinstance(xml, six.text_type):
        xml = xml.encode('utf-8')

    # Call Oozie to validate XML against the schema
    with tempfile.NamedTemporaryFile() as fp:
        # Write XML file
        fp.write(xml)
        fp.flush()

        # Start a process to validate the XML file
        try:
            subprocess.check_output(
                'java -cp lib/oozie-client-4.1.0.jar:lib/commons-cli-1.2.jar '
                'org.apache.oozie.cli.OozieCLI validate {path}'.format(path=fp.name),
                stderr=subprocess.STDOUT,
                shell=True
            )
        except subprocess.CalledProcessError as e:
            raise AssertionError('An XML validation error occurred.\n\n{error}\n\nParsing:\n\n{xml}'.format(
                error=e.output.decode('utf8').strip(),
                xml=xml.decode('utf-8'),
            ))
