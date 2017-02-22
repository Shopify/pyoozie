# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

from pyoozie.xml._coordinator import Coordinator
from pyoozie.xml._tags import Configuration, _validate


def _workflow_submission_xml(username, workflow_xml_path, configuration=None, indent=False):
    """Generate a Workflow XML submission message to POST to Oozie."""
    submission = Configuration(configuration)
    submission.update({
        'user.name': username,
        'oozie.wf.application.path': workflow_xml_path,
    })
    return submission.xml(indent)


def _coordinator_submission_xml(username, coord_xml_path, configuration=None, indent=False):
    """Generate a Coordinator XML submission message to POST to Oozie."""
    submission = Configuration(configuration)
    submission.update({
        'user.name': username,
        'oozie.coord.application.path': coord_xml_path,
    })
    return submission.xml(indent)


class WorkflowBuilder(object):

    def __init__(self, name):
        # Initially, let's just use a static template and only one action payload and one action on error
        self._name = name
        self._action_name = None
        self._action_payload = None
        self._action_error = None
        self._kill_message = None

    def add_action(self, name, action, action_on_error, kill_on_error='${wf:lastErrorNode()} - ${wf:id()}'):
        # Today you can't rename your action and you can only have one, but in the future you can add multiple
        # named actions
        if any((self._action_name, self._action_payload, self._action_error, self._kill_message)):
            raise NotImplementedError("Can only add one action in this version")
        else:
            self._action_name = _validate(name)
            self._action_payload = action
            self._action_error = action_on_error
            self._kill_message = kill_on_error

        return self

    def build(self, indent=False):
        def format_xml(xml):
            xml = xml.replace("<?xml version='1.0' encoding='UTF-8'?>", '')
            return '\n'.join([(' ' * 8) + line for line in xml.strip().split('\n')])
        return '''
<?xml version="1.0" encoding="UTF-8"?>
<workflow-app xmlns="uri:oozie:workflow:0.5"
              name="{name}">
    <start to="action-{action_name}" />
    <action name="action-{action_name}">
{action_payload_xml}
        <ok to="end" />
        <error to="action-error" />
    </action>
    <action name="action-error">
{action_error_xml}
        <ok to="kill" />
        <error to="kill" />
    </action>
    <kill name="kill">
        <message>{kill_message}</message>
    </kill>
    <end name="end" />
</workflow-app>
'''.format(action_payload_xml=format_xml(self._action_payload.xml(indent=indent)),
           action_error_xml=format_xml(self._action_error.xml(indent=indent)),
           kill_message=self._kill_message,
           action_name=self._action_name,
           name=self._name).strip()


class CoordinatorBuilder(object):

    def __init__(self, name, workflow_xml_path, frequency_in_minutes, start, end=None, timezone=None,
                 workflow_configuration=None, timeout_in_minutes=None, concurrency=None, execution_order=None,
                 throttle=None, parameters=None):
        self._coordinator = Coordinator(
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
