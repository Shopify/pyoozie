# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import subprocess
import tempfile

import xmltodict


def xml_to_dict_unordered(xml):
    def unorder(value):
        if hasattr(value, 'items'):
            return {k: unorder(v) for k, v in value.items()}
        elif isinstance(value, list):
            return sorted([unorder(v) for v in value], key=str)
        else:
            return value
    return unorder(xmltodict.parse(xml))


def assert_valid_workflow(xml):
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
            raise AssertionError('An XML validation error\n\n{error}\n\noccurred while parsing:\n\n{xml}'.format(
                error=e.output.decode('utf8').strip(),
                xml=xml,
            ))
