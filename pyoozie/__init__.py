# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

# pylint: disable=import-modules-only

from pyoozie.xml import WorkflowBuilder

from pyoozie.client import OozieClient

from pyoozie.exceptions import OozieException

from pyoozie.model import ArtifactType
from pyoozie.model import CoordinatorActionStatus
from pyoozie.model import CoordinatorStatus
from pyoozie.model import WorkflowActionStatus
from pyoozie.model import WorkflowStatus
from pyoozie.model import parse_coordinator_id
from pyoozie.model import parse_workflow_id

from pyoozie.tags import Configuration
from pyoozie.tags import CoordinatorApp
from pyoozie.tags import Credential
from pyoozie.tags import Email
from pyoozie.tags import ExecutionOrder
from pyoozie.tags import EXEC_FIFO
from pyoozie.tags import EXEC_LAST_ONLY
from pyoozie.tags import EXEC_LIFO
from pyoozie.tags import EXEC_NONE
from pyoozie.tags import GlobalConfiguration
from pyoozie.tags import Parameters
from pyoozie.tags import Shell
from pyoozie.tags import SubWorkflow
from pyoozie.tags import validate_xml_id
from pyoozie.tags import validate_xml_name


__version__ = '0.0.0'

__all__ = (
    # builder
    'WorkflowBuilder',

    # client
    'OozieClient',

    # exceptions
    'OozieException',

    # model
    'ArtifactType',
    'CoordinatorActionStatus',
    'CoordinatorStatus',
    'WorkflowActionStatus',
    'WorkflowStatus',
    'parse_coordinator_id',
    'parse_workflow_id',

    # tags
    'Configuration',
    'CoordinatorApp',
    'Credential',
    'Email',
    'ExecutionOrder',
    'EXEC_FIFO',
    'EXEC_LAST_ONLY',
    'EXEC_LIFO',
    'EXEC_NONE',
    'GlobalConfiguration',
    'Parameters',
    'Shell',
    'SubWorkflow',
    'validate_xml_id',
    'validate_xml_name',
)
