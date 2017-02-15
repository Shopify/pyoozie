# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
import pyoozie.xml


def test_imports():
    # pyoozie.xml._coordinator imports
    from pyoozie.xml import Coordinator, format_datetime

    # Does all contain what we expect?
    expected_all = set(_.__name__ for _ in (Coordinator, format_datetime))
    assert set(pyoozie.xml.__all__) == expected_all
