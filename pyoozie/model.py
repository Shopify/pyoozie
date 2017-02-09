from __future__ import unicode_literals

from collections import namedtuple
from datetime import datetime
import re

from enum import Enum
import untangle

from pyoozie.exceptions import OozieException


_StatusValue = namedtuple('_StatusValue', ['id', 'is_active', 'is_running', 'is_suspendable', 'is_suspended'])


def _status(status_id, is_active=False, is_running=False, is_suspendable=False, is_suspended=False):
    if is_running and not is_active:
        raise OozieException.parse_error("A running status implies active")
    return _StatusValue(status_id, is_active, is_running, is_suspendable, is_suspended)


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
    coord_id, action = parse_coordinator_id(job_id)
    if coord_id and not action:
        return job_id
    raise OozieException.parse_error("Invalid coordinator id: {}".format(job_id))


def _parse_coordinator_action_id(_, job_id):
    coord_id, action = parse_coordinator_id(job_id)
    if coord_id and action:
        return job_id
    raise OozieException.parse_error("Invalid coordinator action id: {}".format(job_id))


def _parse_workflow_id(_, job_id):
    wf_id, action = parse_workflow_id(job_id)
    if wf_id and not action:
        return job_id
    raise OozieException.parse_error("Invalid workflow id: {}".format(job_id))


def _parse_workflow_action_id(_, job_id):
    wf_id, action = parse_workflow_id(job_id)
    if wf_id and action:
        return job_id
    raise OozieException.parse_error("Invalid workflow action id: {}".format(job_id))


def _parse_workflow_parent_id(_, job_id):
    wf_id, action = parse_coordinator_id(job_id)
    if wf_id and action:
        return job_id
    wf_id, action = parse_workflow_id(job_id)
    if wf_id and not action:
        return job_id
    raise OozieException.parse_error("Invalid workflow parent id: {}".format(job_id))


def _parse_time(_, time_string):
    try:
        return datetime.strptime(time_string, '%a, %d %b %Y %H:%M:%S %Z')
    except ValueError as err:
        raise OozieException.parse_error("Error parsing time '{}'".format(time_string), err)


def _parse_configuration(_, conf_string):
    conf = untangle.parse(conf_string).configuration
    return {prop.name.cdata: prop.value.cdata for prop in conf.property}


def _parse_workflow_actions(artifact, actions_list):
    actions = [WorkflowAction(artifact._oozie_api, action, parent=artifact) for action in actions_list]
    return {action.name: action for action in actions}


def _parse_coordinator_actions(artifact, actions_list):
    actions = [CoordinatorAction(artifact._oozie_api, action, parent=artifact) for action in actions_list]
    return {action.actionNumber: action for action in actions}


def _parse_coordinator_status(_, status_string):
    return Coordinator.Status.parse(status_string)


def _parse_coordinator_action_status(_, status_string):
    return CoordinatorAction.Status.parse(status_string)


def _parse_workflow_status(_, status_string):
    return Workflow.Status.parse(status_string)


def _parse_workflow_action_status(_, status_string):
    return WorkflowAction.Status.parse(status_string)


class ArtifactType(Enum):
    Coordinator = 1
    CoordinatorAction = 2
    Workflow = 3
    WorkflowAction = 4


class _OozieArtifact(object):

    REQUIRED_KEYS = {}

    SUPPORTED_KEYS = {'toString': None}

    class StatusEnum(Enum):

        def __str__(self):
            return self.name

        @classmethod
        def as_dict(cls):
            # Since pylint doesn't know about __MEMBERS__
            return {value.name: value for value in cls}

        @classmethod
        def parse(cls, status_string):
            values = cls.as_dict()
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
            return self.value.status_id == 0

        def is_active(self):
            return self.value.is_active

        def is_running(self):
            return self.value.is_running

        def is_suspendable(self):
            return self.value.is_suspendable

        def is_suspended(self):
            return self.value.is_suspended

    def __init__(self, oozie_api, details, parent=None):
        self._oozie_api = oozie_api
        self._parent = parent
        self.toString = None
        details = dict(details)
        for key, func in self.REQUIRED_KEYS.iteritems():
            value = details.pop(key, None)
            try:
                parsed_value = func(self, value) if func and value is not None else value
            except OozieException as err:
                raise OozieException.required_key_missing(key, self, err)
            if parsed_value is None:
                raise OozieException.required_key_missing(key, self)
            else:
                setattr(self, key, parsed_value)
        for key, func in self.SUPPORTED_KEYS.iteritems():
            value = details.pop(key, None)
            value = func(self, value) if func and value is not None else value
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

    class Status(_OozieArtifact.StatusEnum):
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

    def __init__(self, *args, **kwargs):
        # Phony declarations to appease pylint
        self.acl = None
        self.actions = {}
        self.bundleId = None
        self.concurrency = None
        self.conf = None
        self.consoleUrl = None
        self.coordExternalId = None
        self.coordJobId = None
        self.coordJobName = None
        self.coordJobPath = None
        self.endTime = None
        self.executionPolicy = None
        self.frequency = None
        self.group = None
        self.lastAction = None
        self.mat_throttling = None
        self.nextMaterializedTime = None
        self.pauseTime = None
        self.startTime = None
        self.status = None
        self.timeOut = None
        self.timeUnit = None
        self.timeZone = None
        self.toString = None
        self.total = None
        self.user = None
        super(Coordinator, self).__init__(self, *args, **kwargs)

    def fill_in_details(self):
        # Undefined `conf` is probably bad, empty is ok
        if self.conf is None:
            coord = self._oozie_api.job_last_coordinator_info(coordinator_id=self.coordJobId)
            return coord
        else:
            return self

    def _validate_degenerate_fields(self):
        # For any fields that must be in sync, ensure they are.
        # If values are missing, extrapolate them
        if self.toString:
            if self.coordJobId not in self.toString:
                raise OozieException.parse_error("toString does not contain coordinator ID")
            if not self.status.is_unknown() and str(self.status) not in self.toString:
                raise OozieException.parse_error("toString does not contain status")
        else:
            self.toString = 'Coordinator application id[{}] status[{}]'.format(self.coordJobId, self.status)

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
            action = self._oozie_api.job_coordinator_action(action_number=number, coordinator=self)
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

    class Status(_OozieArtifact.StatusEnum):
        UNKNOWN = _status(0)
        FAILED = _status(1)
        IGNORED = _status(2)
        KILLED = _status(3)
        READY = _status(4)
        RUNNING = _status(5, is_active=True, is_running=True, is_suspendable=True)
        SKIPPED = _status(6)
        SUBMITTED = _status(7, is_active=True)
        SUCCEEDED = _status(8)
        SUSPENDED = _status(9, is_active=True, is_running=True, is_suspended=True)
        TIMEDOUT = _status(10)
        WAITING = _status(11)

    def __init__(self, *args, **kwargs):
        # Phony declarations to appease pylint
        self.actionNumber = None
        self.consoleUrl = None
        self.coordJobId = None
        self.createdConf = None
        self.createdTime = None
        self.errorCode = None
        self.errorMessage = None
        self.externalId = None
        self.externalStatus = None
        self.id = None
        self.lastModifiedTime = None
        self.missingDependencies = None
        self.nominalTime = None
        self.pushMissingDependencies = None
        self.runConf = None
        self.status = None
        self.toString = None
        self.trackerUri = None
        self.type = None
        self._parent = None
        self._workflow = None
        super(CoordinatorAction, self).__init__(self, *args, **kwargs)

    def _validate_degenerate_fields(self):
        # For any fields that must be in sync, ensure they are.
        # If values are missing, extrapolate them
        coord_id, action = parse_coordinator_id(self.id)
        if self.coordJobId:
            if self.coordJobId != coord_id:
                raise OozieException.parse_error("coordJobId does not match coordinator action ID")
        else:
            self.coordJobId = coord_id
        if self.actionNumber:
            if str(self.actionNumber) != str(action):
                raise OozieException.parse_error("actionNumber does not match coordinator action ID")
        else:
            self.actionNumber = action
        if self.toString:
            if self.id not in self.toString:
                raise OozieException.parse_error("toString does not contain coordinator action ID")
            if not self.status.is_unknown() and str(self.status) not in self.toString:
                raise OozieException.parse_error("toString does not contain status")
        else:
            self.toString = 'CoordinatorAction name[{}] status[{}]'.format(self.id, self.status)

    def is_coordinator(self):
        return True

    def is_action(self):
        return True

    def workflow(self):
        # TODO: revisit this to support multiple runs
        # Use .../job/...-C@xx?show=allruns to query
        if not self._workflow and self.externalId:
            workflow = self._oozie_api.job_workflow_info(workflow_id=self.externalId)
            if workflow:
                workflow._parent = self
                self._workflow = workflow
                return workflow
            else:
                # Otherwise, *do not* cache the None
                # it might be there next time we ask
                return None

        return self._workflow

    def coordinator(self):
        if not self._parent:
            self._parent = self._oozie_api.job_coordinator_info(coordinator_id=self.coordJobId)
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

    class Status(_OozieArtifact.StatusEnum):
        UNKNOWN = _status(0)
        FAILED = _status(1)
        KILLED = _status(2)
        PREP = _status(3, is_active=True)
        RUNNING = _status(4, is_active=True, is_running=True, is_suspendable=True)
        SUCCEEDED = _status(5)
        SUSPENDED = _status(6, is_active=True, is_running=True, is_suspended=True)

    def __init__(self, *args, **kwargs):
        # Phony declarations to appease pylint
        self.acl = None
        self.actions = {}
        self.appName = None
        self.appPath = None
        self.conf = None
        self.consoleUrl = None
        self.createdTime = None
        self.endTime = None
        self.externalId = None
        self.id = None
        self.group = None
        self.lastModTime = None
        self.parentId = None
        self.run = None
        self.startTime = None
        self.status = None
        self.toString = None
        self.user = None
        self._parent = None
        super(Workflow, self).__init__(self, *args, **kwargs)

    def fill_in_details(self):
        # Undefined `conf` is probably bad, empty is ok
        if self.conf is None:
            workflow = self._oozie_api.job_workflow_info(workflow_id=self.id)
            return workflow
        else:
            return self

    def _validate_degenerate_fields(self):
        # For any fields that must be in sync, ensure they are.
        # If values are missing, extrapolate them
        if self.toString:
            if self.id not in self.toString:
                raise OozieException.parse_error("toString does not contain workflow ID")
            if not self.status.is_unknown() and str(self.status) not in self.toString:
                raise OozieException.parse_error("toString does not contain status")
        else:
            self.toString = 'Workflow id[{}] status[{}]'.format(self.id, self.status)

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
            self._parent = self._oozie_api.job_action_info(self.parentId)
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

    class Status(_OozieArtifact.StatusEnum):
        UNKNOWN = _status(0)
        DONE = _status(1)
        END_MANUAL = _status(2)
        END_RETRY = _status(3)
        ERROR = _status(4)
        FAILED = _status(5)
        KILLED = _status(6)
        OK = _status(7)
        PREP = _status(8)
        RUNNING = _status(9, is_active=True, is_running=True)
        START_MANUAL = _status(10)
        START_RETRY = _status(11)
        USER_RETRY = _status(12, is_active=True)

    def __init__(self, *args, **kwargs):
        # Phony declarations to appease pylint
        self.conf = None
        self.consoleUrl = None
        self.cred = None
        self.data = None
        self.endTime = None
        self.errorCode = None
        self.errorMessage = None
        self.externalChildIDs = None
        self.externalId = None
        self.externalStatus = None
        self.id = None
        self.name = None
        self.retries = None
        self.startTime = None
        self.stats = None
        self.status = None
        self.toString = None
        self.trackerUri = None
        self.transition = None
        self.type = None
        self.userRetryCount = None
        self.userRetryInterval = None
        self.userRetryMax = None
        self._parent = None
        self._subworkflow = None
        super(WorkflowAction, self).__init__(self, *args, **kwargs)

    def _validate_degenerate_fields(self):
        # For any fields that must be in sync, ensure they are.
        # If values are missing, extrapolate them
        _, action = parse_workflow_id(self.id)
        if self.name:
            if self.name != action:
                raise OozieException.parse_error("name does not match workflow action ID")
        else:
            self.name = action
        if self.toString:
            if self.name not in self.toString:
                raise OozieException.parse_error("toString does not contain workflow action name")
            if not self.status.is_unknown() and str(self.status) not in self.toString:
                raise OozieException.parse_error("toString does not contain status")
        else:
            self.toString = 'Action name[{}] status[{}]'.format(self.name, self.status)

    def is_workflow(self):
        return True

    def is_action(self):
        return True

    def subworkflow(self):
        if not self._subworkflow:
            if self.type == 'sub-workflow' and self.externalId:
                workflow = self._oozie_api.job_workflow_info(self.externalId)
                if workflow:
                    workflow._parent = self
                    self._subworkflow = workflow
                    return workflow
                else:
                    # Otherwise, *do not* cache the None
                    # it might be there next time we ask
                    return None

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
            self._parent = self._oozie_api.job_workflow_info(self.externalId)
        return self._parent
