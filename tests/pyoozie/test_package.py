# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
import pyoozie


def test_version():
    assert pyoozie.__version__


def test_imports():
    # pyoozie.xml._coordinator imports
    from pyoozie import ExecutionOrder

    # pyoozie.xml._tags imports
    from pyoozie import Parameters, Configuration, Credentials, Shell, SubWorkflow, GlobalConfiguration, Email

    # pyoozie.xml._builder imports
    from pyoozie import WorkflowBuilder, CoordinatorBuilder

    # Does all contain what we expect?
    expected_all = set(_.__name__ for _ in (ExecutionOrder, Parameters, Configuration, Credentials, Shell,
                                            SubWorkflow, GlobalConfiguration, Email, WorkflowBuilder,
                                            CoordinatorBuilder))
    assert set(pyoozie.__all__) == expected_all
