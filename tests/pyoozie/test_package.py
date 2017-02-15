# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
import pyoozie


def test_version():
    assert pyoozie.__version__


def test_imports():
    # pylint: disable=unused-variable

    # pyoozie.xml._coordinator imports
    from pyoozie import ExecutionOrder  # noqa: ignore=F401

    # pyoozie.xml._tags imports
    from pyoozie import Parameters, Configuration, Credentials, Shell, SubWorkflow  # noqa: ignore=F401
    from pyoozie import GlobalConfiguration, Email  # noqa: ignore=F401

    # pyoozie.xml._builder imports
    from pyoozie import WorkflowBuilder, CoordinatorBuilder  # noqa: ignore=F401
