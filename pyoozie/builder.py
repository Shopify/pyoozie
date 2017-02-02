# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a BSD-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

from pyoozie.coordinator import Coordinator
from pyoozie.tags import Configuration, Parameters


def _workflow_submission_xml(hadoop_user, hdfs_path, configuration=None, indent=False):
    """Generate a Workflow XML submission message to POST to Oozie."""
    submission = Configuration(configuration)
    submission.update({
        'user.name': hadoop_user,
        'oozie.wf.application.path': hdfs_path
    })
    return submission.xml(indent)


def _coordinator_submission_xml(hadoop_user, hdfs_path, configuration=None, indent=False):
    """Generate a Coordinator XML submission message to POST to Oozie."""
    submission = Configuration(configuration)
    submission.update({
        'user.name': hadoop_user,
        'oozie.coord.application.path': hdfs_path,
    })
    return submission.xml(indent)


class workflow(object):

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
        if self._action_name is None and self._action_payload is None and self._action_error is None and \
           self._kill_message is None:
            self._action_name = name
            self._action_payload = action
            self._action_error = action_on_error
            self._kill_message = kill_on_error
        else:
            raise NotImplementedError("Can only add one action in this version")
        return self

    def build(self, indent=False):
        def remove_header(xml):
            return xml.replace("<?xml version='1.0' encoding='UTF-8'?>", '')
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
'''.format(action_payload_xml=remove_header(self._action_payload.xml(indent=indent)),
           action_error_xml=remove_header(self._action_error.xml(indent=indent)),
           kill_message=self._kill_message,
           action_name=self._action_name,
           name=self._name).strip()

    def submit(self, oozie_url, hdfs_path, hadoop_user, hdfs_callback, timeout_in_seconds=None,
               verbose=False, start=False, indent=False):
        xml = self.build(indent=indent)
        hdfs_callback(hdfs_path, xml)
        # TODO create Oozie API and submit
        from mock import Mock
        OozieAPI = Mock()
        api = OozieAPI(url=oozie_url, user=hadoop_user, timeout=timeout_in_seconds, verbose=verbose)
        api.jobs_submit_workflow(hdfs_path=hdfs_path, start=start)
        raise NotImplementedError()


class coordinator(object):

    def __init__(self, name, workflow, frequency_in_minutes, start, end=None, timezone=None,
                 workflow_configuration=None, timeout_in_minutes=None, concurrency=None, execution_order=None,
                 throttle=None, parameters=None):
        workflow_configuration = Configuration(workflow_configuration) if workflow_configuration else None
        self._coordinator = Coordinator(
            name=name,
            workflow_app_path=None,  # Defer this until the build
            frequency=frequency_in_minutes,
            start=start,
            end=end,
            timezone=timezone,
            workflow_configuration=workflow_configuration,
            timeout=timeout_in_minutes,
            concurrency=concurrency,
            execution_order=execution_order,
            throttle=throttle,
            parameters=Parameters(parameters) if parameters else None
        )
        self._workflow = workflow

    def build(self, workflow_hdfs_path, indent=False):
        self._coordinator.workflow_app_path = workflow_hdfs_path
        return self._coordinator.xml(indent)

    def submit(self, oozie_url, workflow_hdfs_path, coord_hdfs_path, hadoop_user, hdfs_callback,
               timeout_in_seconds=None, verbose=False, indent=False):
        self._workflow.submit(
            oozie_url=oozie_url,
            hdfs_path=workflow_hdfs_path,
            hadoop_user=hadoop_user,
            hdfs_callback=hdfs_callback,
            start=False,
            indent=False)
        xml = self.build(workflow_hdfs_path, indent=indent)
        hdfs_callback(coord_hdfs_path, xml)
        # TODO create Oozie API and submit
        from mock import Mock
        OozieAPI = Mock()
        api = OozieAPI(url=oozie_url, user=hadoop_user, timeout=timeout_in_seconds, verbose=verbose)
        api.job_submit_coordinator(hdfs_path=coord_hdfs_path)
        raise NotImplementedError()
