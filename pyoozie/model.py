# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import collections
import datetime
import re
import sys

import enum
import typing  # pylint: disable=unused-import
import untangle

from pyoozie import exceptions


_COORD_ID_RE = re.compile('^(?P<id>.*-C)(?:@(?P<action>[1-9][0-9]*))?$')
_WORKFLOW_ID_RE = re.compile('^(?P<id>.*-W)(?:@(?P<action>.*))?$')


def parse_coordinator_id(string):
    parts = _COORD_ID_RE.match(string) if string else None
    coord_id = parts.group('id') if parts else None
    action = parts.group('action') if parts else None
    action = int(action) if action else None
    return coord_id, action


def parse_workflow_id(string):
    parts = _WORKFLOW_ID_RE.match(string) if string else None
    coord_id = parts.group('id') if parts else None
    action = parts.group('action') if parts else None
    return coord_id, action


def _parse_coordinator_id(_, job_id):
    if job_id:
        coord_id, action = parse_coordinator_id(job_id)
        if coord_id and not action:
            return job_id
        raise exceptions.OozieException.parse_error("Invalid coordinator id: {}".format(job_id))
    return None


def _parse_coordinator_action_id(_, job_id):
    if job_id:
        coord_id, action = parse_coordinator_id(job_id)
        if coord_id and action:
            return job_id
        raise exceptions.OozieException.parse_error("Invalid coordinator action id: {}".format(job_id))
    return None


def _parse_workflow_id(_, job_id):
    if job_id:
        wf_id, action = parse_workflow_id(job_id)
        if wf_id and not action:
            return job_id
        raise exceptions.OozieException.parse_error("Invalid workflow id: {}".format(job_id))
    return None


def _parse_workflow_action_id(_, job_id):
    if job_id:
        wf_id, action = parse_workflow_id(job_id)
        if wf_id and action:
            return job_id
        raise exceptions.OozieException.parse_error("Invalid workflow action id: {}".format(job_id))
    return None


def _parse_workflow_parent_id(_, job_id):
    if job_id:
        wf_id, action = parse_coordinator_id(job_id)
        if wf_id and action:
            return job_id
        wf_id, action = parse_workflow_id(job_id)
        if wf_id and not action:
            return job_id
        raise exceptions.OozieException.parse_error("Invalid workflow parent id: {}".format(job_id))
    return None


def _parse_time(_, time_string):
    if time_string:
        try:
            return datetime.datetime.strptime(time_string, '%a, %d %b %Y %H:%M:%S %Z')
        except ValueError as err:
            raise exceptions.OozieException.parse_error("Error parsing time '{}'".format(time_string), err)
    return None


def _parse_configuration(_, conf_string):
    if conf_string:
        xml = conf_string if sys.version_info >= (3, 0) else conf_string.encode('utf-8')
        conf = untangle.parse(xml).configuration
        return {prop.name.cdata: prop.value.cdata for prop in conf.property}
    return {}


def _parse_workflow_actions(artifact, actions_list):
    actions_list = actions_list or []
    actions = [WorkflowAction(artifact._client, action, parent=artifact) for action in actions_list]
    return {action.name: action for action in actions}


def _parse_coordinator_actions(artifact, actions_list):
    actions_list = actions_list or []
    actions = [CoordinatorAction(artifact._client, action, parent=artifact) for action in actions_list]
    return {action.actionNumber: action for action in actions}


def _parse_coordinator_status(_, status_string):
    return CoordinatorStatus.parse(status_string)


def _parse_coordinator_action_status(_, status_string):
    return CoordinatorActionStatus.parse(status_string)


def _parse_workflow_status(_, status_string):
    return WorkflowStatus.parse(status_string)


def _parse_workflow_action_status(_, status_string):
    return WorkflowActionStatus.parse(status_string)


class ArtifactType(enum.Enum):
    Coordinator = 1
    CoordinatorAction = 2
    Workflow = 3
    WorkflowAction = 4


_StatusValue = collections.namedtuple('_StatusValue',
                                      ['status_id', 'is_active', 'is_running', 'is_suspendable', 'is_suspended'])


def _status(status_id, is_active=False, is_running=False, is_suspendable=False, is_suspended=False):
    if is_running and not is_active:
        raise exceptions.OozieException.parse_error("A running status implies active")
    return _StatusValue(status_id, is_active, is_running, is_suspendable, is_suspended)


class _StatusEnum(enum.Enum):

    def __str__(self):
        return self.name

    @classmethod
    def parse(cls, status_string):
        values = cls.__members__
        return values.get(status_string, values['UNKNOWN'])

    @classmethod
    def active(cls):
        return [status for status in cls if status.is_active()]

    @classmethod
    def running(cls):
        return [status for status in cls if status.is_running()]

    @classmethod
    def suspendable(cls):
        return [status for status in cls if status.is_suspendable()]

    @classmethod
    def suspended(cls):
        return [status for status in cls if status.is_suspended()]

    def is_unknown(self):
        return self._value_.status_id == 0

    def is_active(self):
        return self._value_.is_active

    def is_running(self):
        return self._value_.is_running

    def is_suspendable(self):
        return self._value_.is_suspendable

    def is_suspended(self):
        return self._value_.is_suspended


class CoordinatorStatus(_StatusEnum):
    UNKNOWN = _status(0)
    DONEWITHERROR = _status(1)
    FAILED = _status(2)
    IGNORED = _status(3)
    KILLED = _status(4)
    PAUSED = _status(5, is_active=True)
    PAUSEDWITHERROR = _status(6, is_active=True)
    PREMATER = _status(7, is_active=True)
    PREP = _status(8, is_active=True, is_suspendable=True)
    PREPPAUSED = _status(9, is_active=True)
    PREPSUSPENDED = _status(10, is_active=True, is_suspended=True)
    RUNNING = _status(11, is_active=True, is_running=True, is_suspendable=True)
    RUNNINGWITHERROR = _status(12, is_active=True, is_running=True, is_suspendable=True)
    SUCCEEDED = _status(13)
    SUSPENDED = _status(14, is_active=True, is_running=True, is_suspended=True)
    SUSPENDEDWITHERROR = _status(15, is_active=True, is_running=True, is_suspended=True)


class CoordinatorActionStatus(_StatusEnum):
    UNKNOWN = _status(0)
    FAILED = _status(1)
    IGNORED = _status(2)
    KILLED = _status(3)
    READY = _status(4, is_active=True)
    RUNNING = _status(5, is_active=True, is_running=True, is_suspendable=True)
    SKIPPED = _status(6)
    SUBMITTED = _status(7, is_active=True)
    SUCCEEDED = _status(8)
    SUSPENDED = _status(9, is_active=True, is_running=True, is_suspended=True)
    TIMEDOUT = _status(10)
    WAITING = _status(11, is_active=True)


class WorkflowStatus(_StatusEnum):
    UNKNOWN = _status(0)
    FAILED = _status(1)
    KILLED = _status(2)
    PREP = _status(3, is_active=True)
    RUNNING = _status(4, is_active=True, is_running=True, is_suspendable=True)
    SUCCEEDED = _status(5)
    SUSPENDED = _status(6, is_active=True, is_running=True, is_suspended=True)


class WorkflowActionStatus(_StatusEnum):
    UNKNOWN = _status(0)
    DONE = _status(1)
    END_MANUAL = _status(2)
    END_RETRY = _status(3)
    ERROR = _status(4)
    FAILED = _status(5)
    KILLED = _status(6)
    OK = _status(7)
    PREP = _status(8, is_active=True)
    RUNNING = _status(9, is_active=True, is_running=True)
    START_MANUAL = _status(10)
    START_RETRY = _status(11)
    USER_RETRY = _status(12, is_active=True)


class _OozieArtifact(object):

    REQUIRED_KEYS = {}  # type: typing.Dict[unicode, typing.Callable]

    SUPPORTED_KEYS = {'toString': None}  # type: typing.Dict[unicode, typing.Callable]

    def __init__(self, oozie_client, details, parent=None):
        self._client = oozie_client
        self._parent = parent
        details = dict(details)
        for key, func in self.REQUIRED_KEYS.items():
            value = details.pop(key, None)
            try:
                parsed_value = func(self, value) if func else value
            except exceptions.OozieException as err:
                raise exceptions.OozieException.required_key_missing(key, self, err)
            if parsed_value is None:
                raise exceptions.OozieException.required_key_missing(key, self)
            else:
                setattr(self, key, parsed_value)
        for key, func in self.SUPPORTED_KEYS.items():
            value = details.pop(key, None)
            value = func(self, value) if func else value
            setattr(self, key, value)
        self._details = details
        self._validate_degenerate_fields()

    def __str__(self):
        return self.toString

    def fill_in_details(self):
        # Fetch any missing data not supplied
        return self

    def _validate_degenerate_fields(self):
        # For any fields that must be in sync, ensure they are.
        # If values are missing, extrapolate them
        pass

    def is_coordinator(self):
        return False

    def is_workflow(self):
        return False

    def is_action(self):
        return False


class Coordinator(_OozieArtifact):

    REQUIRED_KEYS = {
        'coordJobId': _parse_coordinator_id,
    }

    SUPPORTED_KEYS = {
        'acl': None,
        'actions': _parse_coordinator_actions,
        'bundleId': None,
        'concurrency': None,
        'conf': _parse_configuration,
        'consoleUrl': None,
        'coordExternalId': None,
        'coordJobName': None,
        'coordJobPath': None,
        'endTime': _parse_time,
        'executionPolicy': None,
        'frequency': None,
        'group': None,
        'lastAction': _parse_time,
        'mat_throttling': None,
        'nextMaterializedTime': _parse_time,
        'pauseTime': None,
        'startTime': _parse_time,
        'status': _parse_coordinator_status,
        'timeOut': None,
        'timeUnit': None,
        'timeZone': None,
        'toString': None,
        'total': None,
        'user': None,
    }

    def __init__(self, *args, **kwargs):
        super(Coordinator, self).__init__(*args, **kwargs)
        self._workflow = None

    def fill_in_details(self):
        # Undefined `conf` is probably bad, empty is ok
        if self.conf is None:
            coord = self._client.job_last_coordinator_info(coordinator_id=self.coordJobId)
            return coord
        else:
            return self

    def _validate_degenerate_fields(self):
        # For any fields that must be in sync, ensure they are.
        # If values are missing, extrapolate them
        self.toString = self.toString or 'Coordinator application id[{}] status[{}]'.format(
            self.coordJobId,
            self.status)
        if self.coordJobId not in self.toString:
            raise exceptions.OozieException.parse_error("toString does not contain coordinator ID")
        if not self.status.is_unknown() and str(self.status) not in self.toString:
            raise exceptions.OozieException.parse_error("toString does not contain status")

    def is_coordinator(self):
        return True

    def coordinator(self):
        return self

    def parent(self):
        return None

    def action(self, number):
        if number in self.actions:
            action = self.actions[number]
        else:
            action = self._client.job_coordinator_action(action_number=number, coordinator=self)
        return action


class CoordinatorAction(_OozieArtifact):

    REQUIRED_KEYS = {
        'id': _parse_coordinator_action_id,
    }

    SUPPORTED_KEYS = {
        'actionNumber': None,
        'consoleUrl': None,
        'coordJobId': _parse_coordinator_id,
        'createdConf': None,
        'createdTime': _parse_time,
        'errorCode': None,
        'errorMessage': None,
        'externalId': _parse_workflow_id,
        'externalStatus': None,
        'lastModifiedTime': _parse_time,
        'missingDependencies': None,
        'nominalTime': _parse_time,
        'pushMissingDependencies': None,
        'runConf': None,
        'status': _parse_coordinator_action_status,
        'toString': None,
        'trackerUri': None,
        'type': None,
    }

    def __init__(self, *args, **kwargs):
        self.status = CoordinatorActionStatus.UNKNOWN
        super(CoordinatorAction, self).__init__(*args, **kwargs)
        self._workflow = None

    def _validate_degenerate_fields(self):
        # For any fields that must be in sync, ensure they are.
        # If values are missing, extrapolate them
        coord_id, action = parse_coordinator_id(self.id)
        self.coordJobId = self.coordJobId or coord_id
        if self.coordJobId != coord_id:
            raise exceptions.OozieException.parse_error("coordJobId does not match coordinator action ID")
        self.actionNumber = self.actionNumber or action
        if self.actionNumber != action:
            raise exceptions.OozieException.parse_error("actionNumber does not match coordinator action ID")
        self.toString = self.toString or 'CoordinatorAction name[{}] status[{}]'.format(self.id, self.status)
        if self.id not in self.toString:
            raise exceptions.OozieException.parse_error("toString does not contain coordinator action ID")
        if not self.status.is_unknown() and str(self.status) not in self.toString:
            raise exceptions.OozieException.parse_error("toString does not contain status")

    def is_coordinator(self):
        return True

    def is_action(self):
        return True

    def workflow(self):
        # TODO: revisit this to support multiple runs
        # Use .../job/...-C@xx?show=allruns to query
        if not self._workflow and self.externalId:
            workflow = self._client.job_workflow_info(workflow_id=self.externalId)
            if workflow:
                workflow._parent = self
                self._workflow = workflow
        return self._workflow

    def coordinator(self):
        if not self._parent:
            self._parent = self._client.job_coordinator_info(coordinator_id=self.coordJobId)
        return self._parent

    def coordinator_action(self):
        return self

    def parent(self):
        return self.coordinator()


class Workflow(_OozieArtifact):

    REQUIRED_KEYS = {
        'id': _parse_workflow_id,
    }

    SUPPORTED_KEYS = {
        'acl': None,
        'actions': _parse_workflow_actions,
        'appName': None,
        'appPath': None,
        'conf': _parse_configuration,
        'consoleUrl': None,
        'createdTime': _parse_time,
        'endTime': _parse_time,
        'externalId': None,
        'group': None,
        'lastModTime': _parse_time,
        'parentId': _parse_workflow_parent_id,
        'run': None,
        'startTime': _parse_time,
        'status': _parse_workflow_status,
        'toString': None,
        'user': None,
    }

    def __init__(self, *args, **kwargs):
        super(Workflow, self).__init__(*args, **kwargs)
        self._workflow = None

    def fill_in_details(self):
        # Undefined `conf` is probably bad, empty is ok
        if self.conf is None:
            workflow = self._client.job_workflow_info(workflow_id=self.id)
            return workflow
        else:
            return self

    def _validate_degenerate_fields(self):
        # For any fields that must be in sync, ensure they are.
        # If values are missing, extrapolate them
        self.toString = self.toString or 'Workflow id[{}] status[{}]'.format(self.id, self.status)
        if self.id not in self.toString:
            raise exceptions.OozieException.parse_error("toString does not contain workflow ID")
        if not self.status.is_unknown() and str(self.status) not in self.toString:
            raise exceptions.OozieException.parse_error("toString does not contain status")

    def is_workflow(self):
        return True

    def coordinator(self):
        parent = self.parent()
        if parent:
            return parent.coordinator()

    def coordinator_action(self):
        parent = self.parent()
        if parent:
            return parent.coordinator_action()

    def parent(self):
        if not self._parent and self.parentId:
            self._parent = self._client.job_action_info(self.parentId)
        return self._parent

    def action(self, name):
        return self.actions.get(name, None)


class WorkflowAction(_OozieArtifact):

    REQUIRED_KEYS = {
        'id': _parse_workflow_action_id,
    }

    SUPPORTED_KEYS = {
        'conf': None,
        'consoleUrl': None,
        'cred': None,
        'data': None,
        'endTime': _parse_time,
        'errorCode': None,
        'errorMessage': None,
        'externalChildIDs': None,
        'externalId': None,
        'externalStatus': None,
        'name': None,
        'retries': None,
        'startTime': _parse_time,
        'stats': None,
        'status': _parse_workflow_action_status,
        'toString': None,
        'trackerUri': None,
        'transition': None,
        'type': None,
        'userRetryCount': None,
        'userRetryInterval': None,
        'userRetryMax': None,
    }

    def __init__(self, *args, **kwargs):
        super(WorkflowAction, self).__init__(*args, **kwargs)
        self._subworkflow = None

    def _validate_degenerate_fields(self):
        # For any fields that must be in sync, ensure they are.
        # If values are missing, extrapolate them
        _, action = parse_workflow_id(self.id)
        self.name = self.name or action
        if self.name != action:
            raise exceptions.OozieException.parse_error("name does not match workflow action ID")
        self.toString = self.toString or 'Action name[{}] status[{}]'.format(self.name, self.status)
        if self.name not in self.toString:
            raise exceptions.OozieException.parse_error("toString does not contain workflow action name")
        if not self.status.is_unknown() and str(self.status) not in self.toString:
            raise exceptions.OozieException.parse_error("toString does not contain status")

    def is_workflow(self):
        return True

    def is_action(self):
        return True

    def subworkflow(self):
        if not self._subworkflow and self.type == 'sub-workflow' and self.externalId:
            workflow = self._client.job_workflow_info(self.externalId)
            if workflow:
                workflow._parent = self
                self._subworkflow = workflow
        return self._subworkflow

    def coordinator(self):
        parent = self.parent()
        if parent:
            return parent.coordinator()

    def coordinator_action(self):
        parent = self.parent()
        if parent:
            return parent.coordinator_action()

    def parent(self):
        if not self._parent and self.externalId:
            self._parent = self._client.job_workflow_info(self.externalId)
        return self._parent
