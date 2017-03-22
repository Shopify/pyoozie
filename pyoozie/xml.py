# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import warnings

from pyoozie import tags


def _workflow_submission_xml(username, workflow_xml_path, configuration=None, indent=False):
    """Generate a Workflow XML submission message to POST to Oozie."""
    submission = tags.Configuration(configuration)
    submission.update({
        'user.name': username,
        'oozie.wf.application.path': workflow_xml_path,
    })
    return submission.xml(indent)


def _coordinator_submission_xml(username, coord_xml_path, configuration=None, indent=False):
    """Generate a Coordinator XML submission message to POST to Oozie."""
    submission = tags.Configuration(configuration)
    submission.update({
        'user.name': username,
        'oozie.coord.application.path': coord_xml_path,
    })
    return submission.xml(indent)


class WorkflowBuilder(object):

    def __init__(self, name, parameters=None, configuration=None, credentials=None, job_tracker=None,
                 name_node=None, job_xml_files=None, default_retry_max=None, default_retry_interval=None,
                 default_retry_policy=None, actions=None):
        # pylint: disable=unused-argument
        self.__name = tags.validate_xml_name(name)
        self.__actions = actions

    def build(self):
        raise NotImplementedError()


class CoordinatorBuilder(object):

    def __init__(self, name, workflow_xml_path, frequency_in_minutes, start, end=None, timezone=None,
                 workflow_configuration=None, timeout_in_minutes=None, concurrency=None, execution_order=None,
                 throttle=None, parameters=None):
        warnings.warn(
            "CoordinatorBuilder is deprecated in favour of pyoozie.CoordinatorApp and will soon be removed",
            DeprecationWarning
        )
        self._coordinator = tags.CoordinatorApp(
            name=name,
            workflow_app_path=workflow_xml_path,
            frequency=frequency_in_minutes,
            start=start,
            end=end,
            timezone=timezone,
            workflow_configuration=workflow_configuration,
            timeout=timeout_in_minutes,
            concurrency=concurrency,
            execution_order=execution_order,
            throttle=throttle,
            parameters=parameters,
        )

    def build(self, indent=False):
        return self._coordinator.xml(indent)
