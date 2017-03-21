# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

from pyoozie import tags
from pyoozie import transforms


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
                 default_retry_policy=None):
        self.__workflow = tags.Workflow(
            name=name,
            parameters=parameters,
            configuration=configuration,
            credentials=credentials,
            job_tracker=job_tracker,
            name_node=name_node,
            job_xml_files=job_xml_files,
            default_retry_max=default_retry_max,
            default_retry_interval=default_retry_interval,
            default_retry_policy=default_retry_policy,
        )
        self.__action_layout_strategy = transforms.serial_layout
        self.__action_layout_kwargs = {}
        self.__actions = {}
        self.__action_kwargs = {}
        self.__dependencies = {}
        self.__transformations = []
        self.__transformation_kwargs = {}

    def set_layout(self, fnc_layout, **kwargs):
        self.__action_layout_strategy = fnc_layout
        self.__action_layout_kwargs = kwargs

    def add_action(self, name, action, depends_upon=None, **kwargs):
        def create_action_name(name):
            return tags.validate_xml_id('action-' + name)

        # Validate name
        name = create_action_name(name)
        assert name not in self.__actions, "Cannot add an action with the same name (%(name)s) twice".format(
            name=name)

        # Compose and validate dependency names
        dependencies = set()
        for dependency_name in depends_upon or {}:
            dependencies.add(create_action_name(dependency_name))
        self.__dependencies[name] = dependencies

        # Store actions
        self.__actions[name] = action
        self.__action_kwargs[name] = kwargs

        return self

    def add_transform(self, fnc_transform, **kwargs):
        assert fnc_transform not in self.__transformation_kwargs, (
            "Cannot add a transformation function (%(fnc)r) twice".format(fnc=fnc_transform))
        self.__transformation_kwargs[fnc_transform] = kwargs
        self.__transformations.append(fnc_transform)
        return self

    def build(self, indent=False):
        env = {
            'actions': self.__actions,
            'action_kwargs': self.__action_kwargs,
            'dependencies': self.__dependencies,
            'workflow': self.__workflow,
        }

        # Ensure that this operation can only be called once
        assert self.__workflow, "Workflow already built"
        self.__workflow = None

        # Layout the action nodes provided
        env = self.__action_layout_strategy(env, **self.__action_layout_kwargs)

        # Manipulate those action nodes (and potentially add others)
        for transformation in self.__transformations:
            transformation_kwargs = self.__transformation_kwargs[transformation]
            env = transformation(env, **transformation_kwargs)

        return env['workflow'].xml(indent=indent)


class CoordinatorBuilder(object):

    def __init__(self, name, workflow_xml_path, frequency_in_minutes, start, end=None, timezone=None,
                 workflow_configuration=None, timeout_in_minutes=None, concurrency=None, execution_order=None,
                 throttle=None, parameters=None):
        self._coordinator = tags.Coordinator(
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
