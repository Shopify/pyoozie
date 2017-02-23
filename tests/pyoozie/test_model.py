# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

from datetime import datetime
from six import string_types
import mock
import pytest

from pyoozie import exceptions
from pyoozie import model


SAMPLE_COORD_ID = '0123456-123456789012345-oozie-oozi-C'
SAMPLE_COORD_ACTION = '0123456-123456789012345-oozie-oozi-C@12'
SAMPLE_WF_ID = '0123456-123456789012345-oozie-oozi-W'
SAMPLE_WF_ACTION = '0123456-123456789012345-oozie-oozi-W@action'
SAMPLE_SUBWF_ACTION = '0123456-123456789012345-oozie-oozi-W@my-sub-workflow'
SAMPLE_START_ACTION = '0123456-123456789012345-oozie-oozi-W@:start:'


@pytest.fixture
def empty_coordinator():
    return {
        'acl': None,
        'actions': None,
        'bundleId': None,
        'concurrency': None,
        'conf': None,
        'consoleUrl': None,
        'coordExternalId': None,
        'coordJobId': None,
        'coordJobName': None,
        'coordJobPath': None,
        'endTime': None,
        'executionPolicy': None,
        'frequency': None,
        'group': None,
        'lastAction': None,
        'mat_throttling': None,
        'nextMaterializedTime': None,
        'pauseTime': None,
        'startTime': None,
        'status': None,
        'timeOut': None,
        'timeUnit': None,
        'timeZone': None,
        'toString': None,
        'total': None,
        'user': None,
        'wat?': 'blarg',
    }


@pytest.fixture
def valid_coordinator():
    return {
        'acl': None,
        'actions': [],
        'bundleId': None,
        'concurrency': 1,
        'conf': '''
<configuration>
    <property>
        <name>key1</name>
        <value>value1</value>
    </property>
    <property>
        <name>key two</name>
        <value>value two</value>
    </property>
</configuration>''',
        'consoleUrl': None,
        'coordExternalId': None,
        'coordJobId': SAMPLE_COORD_ID,
        'coordJobName': 'my-test-coordinator',
        'coordJobPath': '/user/oozie/coordinators/my-test-coordinator',
        'endTime': 'Wed, 13 May 2116 23:42:00 GMT',
        'executionPolicy': 'LAST_ONLY',
        'frequency': '720',
        'group': None,
        'lastAction': 'Fri, 03 Jun 2016 01:00:00 GMT',
        'mat_throttling': 1,
        'nextMaterializedTime': 'Fri, 03 Jun 2016 01:00:00 GMT',
        'pauseTime': None,
        'startTime': 'Sat, 14 May 2016 01:00:00 GMT',
        'status': 'RUNNING',
        'timeOut': 10,
        'timeUnit': 'MINUTE',
        'timeZone': 'UTC',
        'toString': 'Coordinator application id[' + SAMPLE_COORD_ID + '] status[RUNNING]',
        'total': 40,
        'user': 'oozie',
        'wat?': 'blarg',
    }


@pytest.fixture
def sample_coordinator(valid_coordinator):
    return model.Coordinator(mock.Mock(), valid_coordinator, None)


@pytest.fixture
def empty_coordinator_action():
    return {
        'actionNumber': None,
        'consoleUrl': None,
        'coordJobId': None,
        'createdConf': None,
        'createdTime': None,
        'errorCode': None,
        'errorMessage': None,
        'externalId': None,
        'externalStatus': None,
        'id': None,
        'lastModifiedTime': None,
        'missingDependencies': None,
        'nominalTime': None,
        'pushMissingDependencies': None,
        'runConf': None,
        'status': None,
        'toString': None,
        'trackerUri': None,
        'type': None,
        'wat?': 'blarg',
    }


@pytest.fixture
def valid_coordinator_action():
    return {
        'actionNumber': 12,
        'consoleUrl': None,
        'coordJobId': SAMPLE_COORD_ID,
        'createdConf': None,
        'createdTime': 'Thu, 02 Jun 2016 12:58:26 GMT',
        'errorCode': None,
        'errorMessage': None,
        'externalId': SAMPLE_WF_ID,
        'externalStatus': None,
        'id': SAMPLE_COORD_ACTION,
        'lastModifiedTime': 'Thu, 02 Jun 2016 21:40:38 GMT',
        'missingDependencies': None,
        'nominalTime': 'Thu, 02 Jun 2016 13:00:00 GMT',
        'pushMissingDependencies': None,
        'runConf': None,
        'status': 'SUCCEEDED',
        'toString': 'CoordinatorAction name[' + SAMPLE_COORD_ACTION + '] status[SUCCEEDED]',
        'trackerUri': None,
        'type': None,
        'wat?': 'blarg',
    }


@pytest.fixture
def sample_coordinator_action(valid_coordinator_action):
    return model.CoordinatorAction(mock.Mock(), valid_coordinator_action, None)


@pytest.fixture
def empty_workflow():
    return {
        'acl': None,
        'actions': None,
        'appName': None,
        'appPath': None,
        'conf': None,
        'consoleUrl': None,
        'createdTime': None,
        'endTime': None,
        'externalId': None,
        'group': None,
        'id': None,
        'lastModTime': None,
        'parentId': None,
        'run': None,
        'startTime': None,
        'status': None,
        'toString': None,
        'user': None,
        'wat?': 'blarg',
    }


@pytest.fixture
def valid_workflow():
    return {
        'acl': None,
        'actions': [],
        'appName': 'my-test-workflow',
        'appPath': '/user/oozie/workflows/my-test-workflow',
        'conf': '''
<configuration>
    <property>
        <name>key1</name>
        <value>value1</value>
    </property>
    <property>
        <name>key two</name>
        <value>value two</value>
    </property>
</configuration>''',
        'consoleUrl': None,
        'createdTime': 'Thu, 02 Jun 2016 13:16:46 GMT',
        'endTime': 'Thu, 02 Jun 2016 21:40:38 GMT',
        'externalId': None,
        'group': None,
        'id': SAMPLE_WF_ID,
        'lastModTime': 'Thu, 02 Jun 2016 21:40:38 GMT',
        'parentId': SAMPLE_COORD_ACTION,
        'run': 0,
        'startTime': 'Thu, 02 Jun 2016 13:16:46 GMT',
        'status': 'SUCCEEDED',
        'toString': 'Workflow id[' + SAMPLE_WF_ID + '] status[SUCCEEDED]',
        'user': 'oozie',
        'wat?': 'blarg',
    }


@pytest.fixture
def sample_workflow(valid_workflow):
    return model.Workflow(mock.Mock(), valid_workflow, None)


@pytest.fixture
def empty_workflow_action():
    return {
        'conf': None,
        'consoleUrl': None,
        'cred': None,
        'data': None,
        'endTime': None,
        'errorCode': None,
        'errorMessage': None,
        'externalChildIDs': None,
        'externalId': None,
        'externalStatus': None,
        'id': None,
        'name': None,
        'retries': None,
        'startTime': None,
        'stats': None,
        'status': None,
        'toString': None,
        'trackerUri': None,
        'transition': None,
        'type': None,
        'userRetryCount': None,
        'userRetryInterval': None,
        'userRetryMax': None,
        'wat?': 'blarg',
    }


@pytest.fixture
def valid_workflow_action():
    return {
        'conf': '''
<sub-workflow xmlns="uri:oozie:workflow:0.5">
    <app-path>/user/oozie/workflows/currency-lookup</app-path>
    <propagate-configuration />
    <name-node>hdfs://hadoop-production</name-node>
    <job-tracker>yarnRM</job-tracker>
    <configuration />
</sub-workflow>''',
        'consoleUrl': 'http://localhost/oozie?job=' + SAMPLE_WF_ID,
        'cred': 'null',
        'data': None,
        'endTime': 'Thu, 02 Jun 2016 13:23:47 GMT',
        'errorCode': None,
        'errorMessage': None,
        'externalChildIDs': None,
        'externalId': SAMPLE_WF_ID,
        'externalStatus': 'OK',
        'id': SAMPLE_SUBWF_ACTION,
        'name': 'my-sub-workflow',
        'retries': 0,
        'startTime': 'Thu, 02 Jun 2016 13:16:48 GMT',
        'stats': None,
        'status': 'OK',
        'toString': None,
        'trackerUri': 'local',
        'transition': 'joining-b69a73',
        'type': 'sub-workflow',
        'userRetryCount': 0,
        'userRetryInterval': 10,
        'userRetryMax': 0,
        'wat?': 'blarg',
    }


@pytest.fixture
def sample_workflow_action(valid_workflow_action):
    return model.WorkflowAction(mock.Mock(), valid_workflow_action, None)


@pytest.fixture
def valid_start_action():
    return {
        'conf': '',
        'consoleUrl': '-',
        'cred': 'null',
        'data': None,
        'endTime': 'Thu, 02 Jun 2016 13:16:46 GMT',
        'errorCode': None,
        'errorMessage': None,
        'externalChildIDs': None,
        'externalId': None,
        'externalStatus': 'OK',
        'id': SAMPLE_START_ACTION,
        'name': ':start:',
        'retries': 0,
        'startTime': 'Thu, 02 Jun 2016 13:16:46 GMT',
        'stats': None,
        'status': 'OK',
        'toString': None,
        'trackerUri': '-',
        'transition': 'my-sub-workflow',
        'type': ':START:',
        'userRetryCount': 0,
        'userRetryInterval': 10,
        'userRetryMax': 0,
    }


@pytest.fixture
def sample_start_action(valid_start_action):
    return model.WorkflowAction(mock.Mock(), valid_start_action, None)


def test_status():
    value = model._status(1, is_active=False, is_running=False)
    assert not value.is_active
    assert not value.is_running

    value = model._status(2, is_active=True, is_running=False)
    assert value.is_active
    assert not value.is_running

    value = model._status(3, is_active=True, is_running=True)
    assert value.is_active
    assert value.is_running

    with pytest.raises(exceptions.OozieParsingException) as err:
        value = model._status(4, is_active=False, is_running=True)
    assert 'A running status implies active' in str(err)


def test_parse_time():
    result = model._parse_time(None, 'Fri, 01 Jan 2016 01:02:03 GMT')
    assert result == datetime(2016, 1, 1, 1, 2, 3)


def test_parse_configuration():
    conf_string = """
<configuration>
    <property>
        <name>key1</name>
        <value>value1</value>
    </property>
    <property>
        <name>key2</name>
        <value>value2</value>
    </property>
</configuration>
"""
    result = model._parse_configuration(None, conf_string)
    assert result == {'key1': 'value1', 'key2': 'value2'}


@pytest.mark.parametrize("string, expected", [
    ('DONEWITHERROR', model.CoordinatorStatus.DONEWITHERROR),
    ('FAILED', model.CoordinatorStatus.FAILED),
    ('IGNORED', model.CoordinatorStatus.IGNORED),
    ('KILLED', model.CoordinatorStatus.KILLED),
    ('PAUSED', model.CoordinatorStatus.PAUSED),
    ('PAUSEDWITHERROR', model.CoordinatorStatus.PAUSEDWITHERROR),
    ('PREMATER', model.CoordinatorStatus.PREMATER),
    ('PREP', model.CoordinatorStatus.PREP),
    ('PREPPAUSED', model.CoordinatorStatus.PREPPAUSED),
    ('PREPSUSPENDED', model.CoordinatorStatus.PREPSUSPENDED),
    ('RUNNING', model.CoordinatorStatus.RUNNING),
    ('RUNNINGWITHERROR', model.CoordinatorStatus.RUNNINGWITHERROR),
    ('SUCCEEDED', model.CoordinatorStatus.SUCCEEDED),
    ('SUSPENDED', model.CoordinatorStatus.SUSPENDED),
    ('SUSPENDEDWITHERROR', model.CoordinatorStatus.SUSPENDEDWITHERROR),
    ('wat?', model.CoordinatorStatus.UNKNOWN),
])
def test_parse_coordinator_status(string, expected):
    assert model._parse_coordinator_status(None, string) == expected


@pytest.mark.parametrize("string, expected", [
    ('FAILED', model.CoordinatorActionStatus.FAILED),
    ('IGNORED', model.CoordinatorActionStatus.IGNORED),
    ('KILLED', model.CoordinatorActionStatus.KILLED),
    ('READY', model.CoordinatorActionStatus.READY),
    ('RUNNING', model.CoordinatorActionStatus.RUNNING),
    ('SKIPPED', model.CoordinatorActionStatus.SKIPPED),
    ('SUBMITTED', model.CoordinatorActionStatus.SUBMITTED),
    ('SUCCEEDED', model.CoordinatorActionStatus.SUCCEEDED),
    ('SUSPENDED', model.CoordinatorActionStatus.SUSPENDED),
    ('TIMEDOUT', model.CoordinatorActionStatus.TIMEDOUT),
    ('WAITING', model.CoordinatorActionStatus.WAITING),
    ('wat?', model.CoordinatorActionStatus.UNKNOWN),
])
def test_parse_coordinator_action_status(string, expected):
    assert model._parse_coordinator_action_status(None, string) == expected


@pytest.mark.parametrize("string, expected", [
    ('FAILED', model.WorkflowStatus.FAILED),
    ('KILLED', model.WorkflowStatus.KILLED),
    ('PREP', model.WorkflowStatus.PREP),
    ('RUNNING', model.WorkflowStatus.RUNNING),
    ('SUCCEEDED', model.WorkflowStatus.SUCCEEDED),
    ('SUSPENDED', model.WorkflowStatus.SUSPENDED),
    ('wat?', model.WorkflowStatus.UNKNOWN),
])
def test_parse_workflow_status(string, expected):
    assert model._parse_workflow_status(None, string) == expected


@pytest.mark.parametrize("string, expected", [
    ('DONE', model.WorkflowActionStatus.DONE),
    ('END_MANUAL', model.WorkflowActionStatus.END_MANUAL),
    ('END_RETRY', model.WorkflowActionStatus.END_RETRY),
    ('ERROR', model.WorkflowActionStatus.ERROR),
    ('FAILED', model.WorkflowActionStatus.FAILED),
    ('KILLED', model.WorkflowActionStatus.KILLED),
    ('OK', model.WorkflowActionStatus.OK),
    ('PREP', model.WorkflowActionStatus.PREP),
    ('RUNNING', model.WorkflowActionStatus.RUNNING),
    ('START_MANUAL', model.WorkflowActionStatus.START_MANUAL),
    ('START_RETRY', model.WorkflowActionStatus.START_RETRY),
    ('USER_RETRY', model.WorkflowActionStatus.USER_RETRY),
    ('wat?', model.WorkflowActionStatus.UNKNOWN),
])
def test_parse_workflow_action_status(string, expected):
    assert model._parse_workflow_action_status(None, string) == expected


@pytest.mark.parametrize("status, active, running, suspendable, suspended", [
    (model.CoordinatorStatus.UNKNOWN, False, False, False, False),
    (model.CoordinatorStatus.DONEWITHERROR, False, False, False, False),
    (model.CoordinatorStatus.FAILED, False, False, False, False),
    (model.CoordinatorStatus.IGNORED, False, False, False, False),
    (model.CoordinatorStatus.KILLED, False, False, False, False),
    (model.CoordinatorStatus.PAUSED, True, False, False, False),
    (model.CoordinatorStatus.PAUSEDWITHERROR, True, False, False, False),
    (model.CoordinatorStatus.PREMATER, True, False, False, False),
    (model.CoordinatorStatus.PREP, True, False, True, False),
    (model.CoordinatorStatus.PREPPAUSED, True, False, False, False),
    (model.CoordinatorStatus.PREPSUSPENDED, True, False, False, True),
    (model.CoordinatorStatus.RUNNING, True, True, True, False),
    (model.CoordinatorStatus.RUNNINGWITHERROR, True, True, True, False),
    (model.CoordinatorStatus.SUCCEEDED, False, False, False, False),
    (model.CoordinatorStatus.SUSPENDED, True, True, False, True),
    (model.CoordinatorStatus.SUSPENDEDWITHERROR, True, True, False, True),
])
def test_coordinator_status_predicates(status, active, running, suspendable, suspended):
    assert status.is_active() == active
    assert status.is_running() == running
    assert status.is_suspendable() == suspendable
    assert status.is_suspended() == suspended


@pytest.mark.parametrize("status, active, running, suspendable, suspended", [
    (model.CoordinatorActionStatus.UNKNOWN, False, False, False, False),
    (model.CoordinatorActionStatus.FAILED, False, False, False, False),
    (model.CoordinatorActionStatus.IGNORED, False, False, False, False),
    (model.CoordinatorActionStatus.KILLED, False, False, False, False),
    (model.CoordinatorActionStatus.READY, False, False, False, False),
    (model.CoordinatorActionStatus.RUNNING, True, True, True, False),
    (model.CoordinatorActionStatus.SKIPPED, False, False, False, False),
    (model.CoordinatorActionStatus.SUBMITTED, True, False, False, False),
    (model.CoordinatorActionStatus.SUCCEEDED, False, False, False, False),
    (model.CoordinatorActionStatus.SUSPENDED, True, True, False, True),
    (model.CoordinatorActionStatus.TIMEDOUT, False, False, False, False),
    (model.CoordinatorActionStatus.WAITING, False, False, False, False),
])
def test_coordinator_action_status_predicates(status, active, running, suspendable, suspended):
    assert status.is_active() == active
    assert status.is_running() == running
    assert status.is_suspendable() == suspendable
    assert status.is_suspended() == suspended


@pytest.mark.parametrize("status, active, running, suspendable, suspended", [
    (model.WorkflowStatus.SUSPENDED, True, True, False, True),
    (model.WorkflowStatus.SUCCEEDED, False, False, False, False),
    (model.WorkflowStatus.RUNNING, True, True, True, False),
    (model.WorkflowStatus.PREP, True, False, False, False),
    (model.WorkflowStatus.KILLED, False, False, False, False),
    (model.WorkflowStatus.FAILED, False, False, False, False),
    (model.WorkflowStatus.UNKNOWN, False, False, False, False),
])
def test_workflow_status_predicates(status, active, running, suspendable, suspended):
    assert status.is_active() == active
    assert status.is_running() == running
    assert status.is_suspendable() == suspendable
    assert status.is_suspended() == suspended


@pytest.mark.parametrize("status, active, running, suspendable, suspended", [
    (model.WorkflowActionStatus.UNKNOWN, False, False, False, False),
    (model.WorkflowActionStatus.DONE, False, False, False, False),
    (model.WorkflowActionStatus.END_MANUAL, False, False, False, False),
    (model.WorkflowActionStatus.END_RETRY, False, False, False, False),
    (model.WorkflowActionStatus.ERROR, False, False, False, False),
    (model.WorkflowActionStatus.FAILED, False, False, False, False),
    (model.WorkflowActionStatus.KILLED, False, False, False, False),
    (model.WorkflowActionStatus.OK, False, False, False, False),
    (model.WorkflowActionStatus.PREP, False, False, False, False),
    (model.WorkflowActionStatus.RUNNING, True, True, False, False),
    (model.WorkflowActionStatus.START_MANUAL, False, False, False, False),
    (model.WorkflowActionStatus.START_RETRY, False, False, False, False),
    (model.WorkflowActionStatus.USER_RETRY, True, False, False, False),
])
def test_workflow_action_status_predicates(status, active, running, suspendable, suspended):
    assert status.is_active() == active
    assert status.is_running() == running
    assert status.is_suspendable() == suspendable
    assert status.is_suspended() == suspended


def test_parse_empty_coordinator(empty_coordinator):
    with pytest.raises(exceptions.OozieParsingException) as err:
        coord = model.Coordinator(None, empty_coordinator, None)
    assert "Required key 'coordJobId' missing or invalid in Coordinator" in str(err)

    empty_coordinator['coordJobId'] = 'bad-coord-id'
    with pytest.raises(exceptions.OozieParsingException) as err:
        coord = model.Coordinator(None, empty_coordinator, None)
    assert "Required key 'coordJobId' missing or invalid in Coordinator" in str(err)

    empty_coordinator['coordJobId'] = SAMPLE_COORD_ID
    coord = model.Coordinator(None, empty_coordinator, None)
    assert coord.coordJobId == SAMPLE_COORD_ID
    assert coord._details == {'wat?': 'blarg'}


def test_parse_coordinator(valid_coordinator):
    coord = model.Coordinator(None, valid_coordinator, None)
    assert coord.conf == {
        'key1': 'value1',
        'key two': 'value two',
    }
    assert coord.coordJobId == SAMPLE_COORD_ID
    assert coord.coordJobName == 'my-test-coordinator'
    assert coord.startTime == datetime(2016, 5, 14, 1, 0, 0)
    assert coord.endTime == datetime(2116, 5, 13, 23, 42, 0)
    assert coord.lastAction == datetime(2016, 6, 3, 1, 0, 0)
    assert coord.nextMaterializedTime == datetime(2016, 6, 3, 1, 0, 0)
    assert coord.status == model.CoordinatorStatus.RUNNING
    assert coord._details == {'wat?': 'blarg'}


def test_coordinator_validate_degenerate_fields(empty_coordinator):
    empty_coordinator['coordJobId'] = SAMPLE_COORD_ID
    empty_coordinator['status'] = 'RUNNING'
    empty_coordinator['toString'] = 'Coordinator application id[blarg] status [RUNNING]'
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.Coordinator(None, empty_coordinator, None)
    assert 'toString does not contain coordinator ID' in str(err)

    empty_coordinator['status'] = 'KILLED'
    empty_coordinator['toString'] = 'Coordinator application id[' + SAMPLE_COORD_ID + '] status [RUNNING]'
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.Coordinator(None, empty_coordinator, None)
    assert 'toString does not contain status' in str(err)


def test_coordinator_extrapolate_degenerate_fields(empty_coordinator):
    empty_coordinator['coordJobId'] = SAMPLE_COORD_ID
    empty_coordinator['status'] = 'RUNNING'
    coord = model.Coordinator(None, empty_coordinator, None)
    assert coord.toString == 'Coordinator application id[' + SAMPLE_COORD_ID + '] status[RUNNING]'


def test_parse_empty_coordinator_action(empty_coordinator_action):
    with pytest.raises(exceptions.OozieParsingException) as err:
        coord = model.CoordinatorAction(None, empty_coordinator_action, None)
    assert "Required key 'id' missing or invalid in CoordinatorAction" in str(err)

    empty_coordinator_action['id'] = 'bad-coord-C@should-be-int'
    with pytest.raises(exceptions.OozieParsingException) as err:
        coord = model.CoordinatorAction(None, empty_coordinator_action, None)
    assert "Required key 'id' missing or invalid in CoordinatorAction" in str(err)

    empty_coordinator_action['id'] = SAMPLE_COORD_ACTION
    coord = model.CoordinatorAction(None, empty_coordinator_action, None)
    assert coord.id == SAMPLE_COORD_ACTION
    assert coord._details == {'wat?': 'blarg'}


def test_parse_coordinator_action(valid_coordinator_action):
    coord = model.CoordinatorAction(None, valid_coordinator_action, None)
    assert coord.actionNumber == 12
    assert coord.id == SAMPLE_COORD_ACTION
    assert coord.coordJobId == SAMPLE_COORD_ID
    assert coord.externalId == SAMPLE_WF_ID
    assert coord.createdTime == datetime(2016, 6, 2, 12, 58, 26)
    assert coord.lastModifiedTime == datetime(2016, 6, 2, 21, 40, 38)
    assert coord.nominalTime == datetime(2016, 6, 2, 13, 0, 0)
    assert coord.status == model.CoordinatorActionStatus.SUCCEEDED
    assert coord._details == {'wat?': 'blarg'}


def test_coordinator_action_validate_degenerate_fields(empty_coordinator_action):
    empty_coordinator_action['id'] = SAMPLE_COORD_ACTION
    empty_coordinator_action['coordJobId'] = 'bad-coord-C'
    empty_coordinator_action['actionNumber'] = 666
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.CoordinatorAction(None, empty_coordinator_action, None)
    assert 'coordJobId does not match coordinator action ID' in str(err)

    empty_coordinator_action['coordJobId'] = SAMPLE_COORD_ID
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.CoordinatorAction(None, empty_coordinator_action, None)
    assert 'actionNumber does not match coordinator action ID' in str(err)
    empty_coordinator_action['actionNumber'] = 12

    empty_coordinator_action['id'] = SAMPLE_COORD_ACTION
    empty_coordinator_action['status'] = 'RUNNING'
    empty_coordinator_action['toString'] = 'CoordinatorAction name[blarg] status [RUNNING]'
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.CoordinatorAction(None, empty_coordinator_action, None)
    assert 'toString does not contain coordinator action ID' in str(err)

    empty_coordinator_action['status'] = 'KILLED'
    empty_coordinator_action['toString'] = 'CoordinatorAction name[' + SAMPLE_COORD_ACTION + '] status [RUNNING]'
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.CoordinatorAction(None, empty_coordinator_action, None)
    assert 'toString does not contain status' in str(err)


def test_coordinator_action_extrapolate_degenerate_fields(empty_coordinator_action):
    empty_coordinator_action['id'] = SAMPLE_COORD_ACTION
    coord = model.CoordinatorAction(None, empty_coordinator_action, None)
    assert coord.coordJobId == SAMPLE_COORD_ID
    assert coord.actionNumber == 12

    empty_coordinator_action['status'] = 'RUNNING'
    coord = model.CoordinatorAction(None, empty_coordinator_action, None)
    assert coord.toString == 'CoordinatorAction name[' + SAMPLE_COORD_ACTION + '] status[RUNNING]'


def test_parse_empty_workflow(empty_workflow):
    with pytest.raises(exceptions.OozieParsingException) as err:
        wf = model.Workflow(None, empty_workflow, None)
    assert "Required key 'id' missing or invalid in Workflow" in str(err)

    empty_workflow['id'] = 'bad-wf-id'
    with pytest.raises(exceptions.OozieParsingException) as err:
        wf = model.Workflow(None, empty_workflow, None)
    assert "Required key 'id' missing or invalid in Workflow" in str(err)

    empty_workflow['id'] = SAMPLE_WF_ID
    wf = model.Workflow(None, empty_workflow, None)
    assert wf.id == SAMPLE_WF_ID
    assert wf._details == {'wat?': 'blarg'}


def test_parse_workflow(valid_workflow):
    wf = model.Workflow(None, valid_workflow, None)
    assert wf.id == SAMPLE_WF_ID
    assert wf.appName == 'my-test-workflow'
    assert wf.createdTime == datetime(2016, 6, 2, 13, 16, 46)
    assert wf.startTime == datetime(2016, 6, 2, 13, 16, 46)
    assert wf.endTime == datetime(2016, 6, 2, 21, 40, 38)
    assert wf.lastModTime == datetime(2016, 6, 2, 21, 40, 38)
    assert wf.status == model.WorkflowStatus.SUCCEEDED
    assert wf._details == {'wat?': 'blarg'}


def test_workflow_validate_degenerate_fields(empty_workflow):
    empty_workflow['id'] = SAMPLE_WF_ID
    empty_workflow['status'] = 'RUNNING'
    empty_workflow['toString'] = 'Workflow id[blarg] status [RUNNING]'
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.Workflow(None, empty_workflow, None)
    assert 'toString does not contain workflow ID' in str(err)

    empty_workflow['status'] = 'KILLED'
    empty_workflow['toString'] = 'Workflow id[' + SAMPLE_WF_ID + '] status[RUNNING]'
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.Workflow(None, empty_workflow, None)
    assert 'toString does not contain status' in str(err)


def test_workflow_extrapolate_degenerate_fields(empty_workflow):
    empty_workflow['id'] = SAMPLE_WF_ID
    empty_workflow['status'] = 'RUNNING'
    wf = model.Workflow(None, empty_workflow, None)
    assert wf.toString == 'Workflow id[' + SAMPLE_WF_ID + '] status[RUNNING]'


def test_parse_empty_workflow_action(empty_workflow_action):
    with pytest.raises(exceptions.OozieParsingException) as err:
        wf = model.WorkflowAction(None, empty_workflow_action, None)
    assert "Required key 'id' missing or invalid in WorkflowAction" in str(err)

    empty_workflow_action['id'] = 'bad-wf-action@foo'
    with pytest.raises(exceptions.OozieParsingException) as err:
        wf = model.WorkflowAction(None, empty_workflow_action, None)
    assert "Required key 'id' missing or invalid in WorkflowAction" in str(err)

    empty_workflow_action['id'] = SAMPLE_WF_ACTION
    wf = model.WorkflowAction(None, empty_workflow_action, None)
    assert wf.id == SAMPLE_WF_ACTION
    assert wf._details == {'wat?': 'blarg'}


def test_parse_workflow_action(valid_workflow_action):
    wf = model.WorkflowAction(None, valid_workflow_action, None)
    assert wf.id == SAMPLE_SUBWF_ACTION
    assert wf.name == 'my-sub-workflow'
    assert wf.startTime == datetime(2016, 6, 2, 13, 16, 48)
    assert wf.endTime == datetime(2016, 6, 2, 13, 23, 47)
    assert wf.status == model.WorkflowActionStatus.OK
    assert isinstance(wf.conf, string_types)  # Does NOT get parsed
    assert wf._details == {'wat?': 'blarg'}


def test_workflow_action_validate_degenerate_fields(empty_workflow_action):
    empty_workflow_action['id'] = SAMPLE_WF_ACTION
    empty_workflow_action['name'] = 'bad-action'
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.WorkflowAction(None, empty_workflow_action, None)
    assert 'name does not match workflow action ID' in str(err)

    empty_workflow_action['name'] = 'action'
    empty_workflow_action['status'] = 'OK'
    empty_workflow_action['toString'] = 'Action name[blarg] status[OK]'
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.WorkflowAction(None, empty_workflow_action, None)
    assert 'toString does not contain workflow action name' in str(err)

    empty_workflow_action['status'] = 'ERROR'
    empty_workflow_action['toString'] = 'Action name[' + SAMPLE_WF_ACTION + '] status[OK]'
    with pytest.raises(exceptions.OozieParsingException) as err:
        model.WorkflowAction(None, empty_workflow_action, None)
    assert 'toString does not contain status' in str(err)


def test_workflow_action_extrapolate_degenerate_fields(empty_workflow_action):
    empty_workflow_action['id'] = SAMPLE_WF_ACTION
    action = model.WorkflowAction(None, empty_workflow_action, None)
    assert action.name == 'action'

    empty_workflow_action['status'] = 'OK'
    action = model.WorkflowAction(None, empty_workflow_action, None)
    assert action.toString == 'Action name[action] status[OK]'


def test_coordinator_coordinator(sample_coordinator):
    assert sample_coordinator.coordinator() == sample_coordinator


def test_coordinator_parent(sample_coordinator):
    assert sample_coordinator.parent() is None


def test_coordinator_action(sample_coordinator, sample_coordinator_action):
    mock_client = sample_coordinator._client

    sample_coordinator.actions = {12: sample_coordinator_action}
    action = sample_coordinator.action(12)
    assert action == sample_coordinator_action
    assert not mock_client.job_action_info.called

    action = sample_coordinator.action(3)
    mock_client.job_coordinator_action.assert_called_with(action_number=3, coordinator=sample_coordinator)


def test_coordinator_action_coordinator(sample_coordinator_action, sample_coordinator):
    mock_client = sample_coordinator_action._client

    sample_coordinator_action._parent = sample_coordinator
    coord = sample_coordinator_action.coordinator()
    assert coord == sample_coordinator
    assert not mock_client.job_info.called

    sample_coordinator_action._parent = None
    coord = sample_coordinator_action.coordinator()
    mock_client.job_coordinator_info.assert_called_with(coordinator_id=SAMPLE_COORD_ID)


def test_coordinator_action_parent(sample_coordinator_action, sample_coordinator):
    mock_client = sample_coordinator_action._client

    sample_coordinator_action._parent = sample_coordinator
    coord = sample_coordinator_action.coordinator()
    assert coord == sample_coordinator
    assert not mock_client.job_info.called

    sample_coordinator_action._parent = None
    coord = sample_coordinator_action.coordinator()
    mock_client.job_coordinator_info.assert_called_with(coordinator_id=SAMPLE_COORD_ID)


def test_workflow_coordinator(sample_workflow):
    mock_client = sample_workflow._client
    sample_workflow.coordinator()
    mock_client.job_action_info.assert_called_with(SAMPLE_COORD_ACTION)
    assert mock_client.job_action_info().coordinator.called


def test_workflow_parent(sample_workflow, sample_coordinator):
    mock_client = sample_workflow._client

    sample_workflow._parent = sample_coordinator
    coord = sample_workflow.parent()
    assert coord == sample_coordinator
    assert not mock_client.job_info.called

    sample_workflow._parent = None
    coord = sample_workflow.parent()
    mock_client.job_action_info.assert_called_with(SAMPLE_COORD_ACTION)
    assert sample_workflow._parent == coord


def test_workflow_action(sample_workflow, sample_workflow_action):
    assert sample_workflow.action('action') is None

    sample_workflow.actions = {'my-sub-workflow': sample_workflow_action}
    assert sample_workflow.action('my-sub-workflow') == sample_workflow_action


def test_workflow_action_coordinator(sample_workflow_action):
    sample_workflow_action._parent = mock.Mock()
    sample_workflow_action.coordinator()
    assert sample_workflow_action._parent.coordinator.called


def test_workflow_action_parent(sample_workflow_action, sample_workflow):
    mock_client = sample_workflow_action._client

    sample_workflow_action._parent = sample_workflow
    wf = sample_workflow_action.parent()
    assert wf == sample_workflow
    assert not mock_client.job_workflow_info.called

    sample_workflow_action._parent = None
    wf = sample_workflow_action.parent()
    mock_client.job_workflow_info.assert_called_with(SAMPLE_WF_ID)
    assert sample_workflow_action._parent == wf


def test_workflow_action_subworkflow(sample_workflow_action):
    mock_client = sample_workflow_action._client
    swf = sample_workflow_action.subworkflow()
    mock_client.job_workflow_info.assert_called_with(SAMPLE_WF_ID)
    assert swf._parent == sample_workflow_action


def test_start_action_subworkflow(sample_start_action):
    mock_client = sample_start_action._client
    swf = sample_start_action.subworkflow()
    assert swf is None
    assert not mock_client.job_workflow_info.called


def test_job_is_coordinator(sample_coordinator, sample_coordinator_action, sample_workflow, sample_workflow_action):
    assert sample_coordinator.is_coordinator()
    assert sample_coordinator_action.is_coordinator()
    assert not sample_workflow.is_coordinator()
    assert not sample_workflow_action.is_coordinator()


def test_job_is_workflow(sample_coordinator, sample_coordinator_action, sample_workflow, sample_workflow_action):
    assert not sample_coordinator.is_workflow()
    assert not sample_coordinator_action.is_workflow()
    assert sample_workflow.is_workflow()
    assert sample_workflow_action.is_workflow()


def test_job_is_action(sample_coordinator, sample_coordinator_action, sample_workflow, sample_workflow_action):
    assert not sample_coordinator.is_action()
    assert sample_coordinator_action.is_action()
    assert not sample_workflow.is_action()
    assert sample_workflow_action.is_action()
