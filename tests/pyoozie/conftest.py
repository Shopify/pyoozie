# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
import pytest


@pytest.fixture
def coordinator_xml_with_controls():
    with open('tests/data/coordinator-with-controls.xml', 'r') as fh:
        return fh.read()
