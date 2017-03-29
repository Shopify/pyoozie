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

    def __init__(self, name):
        warnings.warn("WorkflowBuilder will be replaced in the future with a different API to construct workflows",
                      PendingDeprecationWarning)

        # Initially, let's just use a static template and only one action payload and one action on error
        self._name = tags.validate_xml_name(name)
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
            self._action_name = tags.validate_xml_id('action-' + name)
            self._action_payload = action
            self._action_error = action_on_error
            self._kill_message = kill_on_error

        return self

    def build(self, indent=False):
        def format_xml(xml):
            xml = xml.decode('utf-8').replace("<?xml version='1.0' encoding='UTF-8'?>", '')
            return '\n'.join([(' ' * 8) + line for line in xml.strip().split('\n')])
        return '''
<?xml version="1.0" encoding="UTF-8"?>
<workflow-app xmlns="uri:oozie:workflow:0.5"
              name="{name}">
    <start to="{action_name}" />
    <action name="{action_name}">
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
           name=self._name).strip().encode('utf-8')
