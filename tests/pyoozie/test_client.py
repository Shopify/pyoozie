# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import copy
import mock
import pytest
import requests_mock

from pyoozie import exceptions
from pyoozie import model
from pyoozie import client
from pyoozie import xml


# TODO: share these with test_model.py?
SAMPLE_COORD_ID = '0123456-123456789012345-oozie-oozi-C'
SAMPLE_COORD_ACTION = '0123456-123456789012345-oozie-oozi-C@12'
SAMPLE_WF_ID = '0123456-123456789012345-oozie-oozi-W'
SAMPLE_WF_ACTION = '0123456-123456789012345-oozie-oozi-W@foo'


@pytest.fixture
def oozie_config():
    return {
        'url': 'http://localhost:11000/oozie',
        'user': 'oozie',
        'timeout': 30,
        'verbose': False,
        'launcher_memory_in_mb': '5000',
        'launcher_queue': 'test.ignore',
    }


@pytest.fixture
def api(oozie_config):
    with mock.patch('pyoozie.client.OozieClient._test_connection'):
        yield client.OozieClient(**oozie_config)


@pytest.fixture
def sample_coordinator_running(api):
    info = {
        'coordJobId': SAMPLE_COORD_ID,
        'status': 'RUNNING'
    }
    return model.Coordinator(api, info, None)


@pytest.fixture
def sample_coordinator_suspended(api):
    info = {
        'coordJobId': SAMPLE_COORD_ID,
        'status': 'SUSPENDED'
    }
    return model.Coordinator(api, info, None)


@pytest.fixture
def sample_coordinator_killed(api):
    info = {
        'coordJobId': SAMPLE_COORD_ID,
        'status': 'KILLED'
    }
    return model.Coordinator(api, info, None)


@pytest.fixture
def sample_coordinator_action_running(api, sample_coordinator_running):
    info = {
        'id': SAMPLE_COORD_ACTION,
        'status': 'RUNNING'
    }
    action = model.CoordinatorAction(api, info, sample_coordinator_running)
    action.parent().actions = {12: action}
    return action


@pytest.fixture
def sample_coordinator_action_suspended(api, sample_coordinator_running):
    info = {
        'id': SAMPLE_COORD_ACTION,
        'status': 'SUSPENDED'
    }
    action = model.CoordinatorAction(api, info, sample_coordinator_running)
    action.parent().actions = {12: action}
    return action


@pytest.fixture
def sample_coordinator_action_killed(api, sample_coordinator_running):
    info = {
        'id': SAMPLE_COORD_ACTION,
        'status': 'KILLED'
    }
    action = model.CoordinatorAction(api, info, sample_coordinator_running)
    action.parent().actions = {12: action}
    return action


@pytest.fixture
def sample_coordinator_action_killed_with_killed_coordinator(api, sample_coordinator_killed):
    info = {
        'id': SAMPLE_COORD_ACTION,
        'status': 'KILLED'
    }
    action = model.CoordinatorAction(api, info, sample_coordinator_killed)
    action.parent().actions = {12: action}
    return action


@pytest.fixture
def sample_workflow_running(api):
    info = {
        'id': SAMPLE_WF_ID,
        'status': 'RUNNING'
    }
    return model.Workflow(api, info, None)


@pytest.fixture
def sample_workflow_suspended(api):
    info = {
        'id': SAMPLE_WF_ID,
        'status': 'SUSPENDED'
    }
    return model.Workflow(api, info, None)


@pytest.fixture
def sample_workflow_killed(api):
    info = {
        'id': SAMPLE_WF_ID,
        'status': 'KILLED'
    }
    return model.Workflow(api, info, None)


@pytest.fixture
def sample_workflow_prep(api):
    info = {
        'id': SAMPLE_WF_ID,
        'status': 'PREP'
    }
    return model.Workflow(api, info, None)


class TestOozieClientCore(object):

    @mock.patch('pyoozie.client.OozieClient._test_connection')
    def test_construction(self, mock_test_conn, oozie_config):
        api = client.OozieClient(**oozie_config)
        assert not mock_test_conn.called
        assert api._url == 'http://localhost:11000/oozie'

    def test_test_connection(self, oozie_config):
        with requests_mock.mock() as m:
            m.get('http://localhost:11000/oozie/versions', text='[0, 1, 2]')
            client.OozieClient(**oozie_config)._test_connection()

            m.get('http://localhost:11000/oozie/versions', text='[0, 1]')
            with pytest.raises(exceptions.OozieException) as err:
                client.OozieClient(**oozie_config)._test_connection()
            assert 'does not support API version 2' in str(err)

            m.get('http://localhost:11000/oozie/versions', status_code=404)
            with pytest.raises(exceptions.OozieException) as err:
                client.OozieClient(**oozie_config)._test_connection()
            assert 'Unable to contact Oozie server' in str(err)

            m.get('http://localhost:11000/oozie/versions', text='>>> fail <<<')
            with pytest.raises(exceptions.OozieException) as err:
                client.OozieClient(**oozie_config)._test_connection()
            assert 'Invalid response from Oozie server' in str(err)

    def test_test_connection_is_called_once(self, oozie_config):
        with requests_mock.mock() as m:
            m.get('http://localhost:11000/oozie/v2/admin/build-version', text='{}')

            with mock.patch('pyoozie.client.OozieClient._test_connection') as m_test:
                oozie_client = client.OozieClient(**oozie_config)
                oozie_client.admin_build_version()
                oozie_client.admin_build_version()
                m_test.assert_called_once_with()

    def test_request(self, api):
        with requests_mock.mock() as m:
            m.get('http://localhost:11000/oozie/v2/endpoint', text='{"result": "pass"}')
            result = api._request('GET', 'endpoint', None, None)
            assert result['result'] == 'pass'

        with requests_mock.mock() as m:
            m.get('http://localhost:11000/oozie/v2/endpoint')
            result = api._request('GET', 'endpoint', None, None)
            assert result is None

        with requests_mock.mock() as m:
            m.get('http://localhost:11000/oozie/v2/endpoint', text='>>> fail <<<')
            with pytest.raises(exceptions.OozieException) as err:
                api._request('GET', 'endpoint', None, None)
            assert 'Invalid response from Oozie server' in str(err)

    def test_get(self, api):
        with requests_mock.mock() as m:
            m.get('http://localhost:11000/oozie/v2/endpoint', text='{"result": "pass"}')
            result = api._get('endpoint')
            assert result['result'] == 'pass'

    def test_put(self, api):
        with requests_mock.mock() as m:
            headers = {'Content-Type': 'application/xml'}
            m.put('http://localhost:11000/oozie/v2/endpoint', request_headers=headers)
            result = api._put('endpoint')
            assert result is None

    def test_post(self, api):
        with requests_mock.mock() as m:
            headers = {'Content-Type': 'application/xml'}
            m.post('http://localhost:11000/oozie/v2/endpoint', request_headers=headers, text='{"result": "pass"}')
            result = api._post('endpoint', content='<xml/>')
            assert result['result'] == 'pass'

    def test_headers(self, api):
        headers = api._headers()
        assert headers == {}

        headers = api._headers(content_type='foo/bar')
        assert headers == {'Content-Type': 'foo/bar'}


class TestOozieClientAdmin(object):

    @pytest.mark.parametrize("function, endpoint", [
        ('admin_status', 'status'),
        ('admin_os_env', 'os-env'),
        ('admin_java_properties', 'java-sys-properties'),
        ('admin_configuration', 'configuration'),
        ('admin_instrumentation', 'instrumentation'),
        ('admin_metrics', 'metrics'),
        ('admin_build_version', 'build-version'),
        ('admin_available_timezones', 'available-timezones'),
        ('admin_queue_dump', 'queue-dump'),
        ('admin_available_oozie_servers', 'available-oozie-servers'),
    ])
    def test_simple_admin(self, function, endpoint, api):
        with mock.patch.object(api, '_get', return_value=True) as mock_get:
            assert api.__getattribute__(function)()
            mock_get.assert_called_with('admin/' + endpoint)

    def test_admin_list_sharelib(self, api):
        reply = {
            'sharelib': [
                {'name': 'oozie'},
                {'name': 'hive'},
                {'name': 'distcp'},
                {'name': 'hcatalog'},
                {'name': 'sqoop'},
                {'name': 'mapreduce-streaming'},
                {'name': 'spark'},
                {'name': 'hive2'},
                {'name': 'pig'}
            ]
        }
        expected = ['oozie', 'hive', 'distcp', 'hcatalog', 'sqoop', 'mapreduce-streaming', 'spark', 'hive2', 'pig']
        with mock.patch.object(api, '_get', return_value=reply) as mock_get:
            assert api.admin_list_sharelib() == expected
            mock_get.assert_called_with('admin/list_sharelib')

    def test_admin_list_all_sharelib(self, api):
        libs = {
            'admin/list_sharelib?lib=oozie': {'sharelib': [{'files': ['oozie1', 'oozie2'], 'name': 'oozie'}]},
            'admin/list_sharelib?lib=distcp': {'sharelib': [{'files': ['distcp1', 'distcp2'], 'name': 'distcp'}]},
        }
        expected = {
            'oozie': ['oozie1', 'oozie2'],
            'distcp': ['distcp1', 'distcp2'],
        }
        with mock.patch.object(api, 'admin_list_sharelib', return_value=['oozie', 'distcp']):
            with mock.patch.object(api, '_get') as mock_get:
                mock_get.side_effect = lambda endpoint: libs[endpoint]
                result = api.admin_list_all_sharelib()
                assert result == expected


class TestOozieClientJobsQuery(object):

    def test_jobs_query_workflow_parameters(self, api):
        mock_result = {
            'total': 0,
            'workflows': []
        }
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.return_value = mock_result

            api._jobs_query(model.ArtifactType.Workflow)
            mock_get.assert_called_with('jobs?jobtype=wf&offset=1&len=5000')

            api._jobs_query(model.ArtifactType.Workflow, user='john_doe')
            mock_get.assert_called_with('jobs?jobtype=wf&filter=user=john_doe&offset=1&len=5000')

            api._jobs_query(model.ArtifactType.Workflow, name='my_workflow')
            mock_get.assert_called_with('jobs?jobtype=wf&filter=name=my_workflow&offset=1&len=5000')

            api._jobs_query(model.ArtifactType.Workflow, status=model.WorkflowStatus.RUNNING)
            mock_get.assert_called_with('jobs?jobtype=wf&filter=status=RUNNING&offset=1&len=5000')

            api._jobs_query(model.ArtifactType.Workflow, status=model.WorkflowStatus.running())
            mock_get.assert_called_with('jobs?jobtype=wf&filter=status=RUNNING;status=SUSPENDED&offset=1&len=5000')

            api._jobs_query(
                model.ArtifactType.Workflow,
                user='john_doe',
                name='my_workflow',
                status=model.WorkflowStatus.running())
            mock_get.assert_called_with('jobs?jobtype=wf&filter=user=john_doe;name=my_workflow;status=RUNNING;'
                                        'status=SUSPENDED&offset=1&len=5000')

    def test_jobs_query_coordinator_parameters(self, api):
        mock_result = {
            'total': 0,
            'coordinatorjobs': []
        }
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.return_value = mock_result

            api._jobs_query(model.ArtifactType.Coordinator)
            mock_get.assert_called_with('jobs?jobtype=coordinator&offset=1&len=5000')

            api._jobs_query(model.ArtifactType.Coordinator, user='john_doe')
            mock_get.assert_called_with('jobs?jobtype=coordinator&filter=user=john_doe&offset=1&len=5000')

            api._jobs_query(model.ArtifactType.Coordinator, name='my_coordinator')
            mock_get.assert_called_with('jobs?jobtype=coordinator&filter=name=my_coordinator&offset=1&len=5000')

            api._jobs_query(model.ArtifactType.Coordinator, status=model.CoordinatorStatus.RUNNING)
            mock_get.assert_called_with('jobs?jobtype=coordinator&filter=status=RUNNING&offset=1&len=5000')

            api._jobs_query(model.ArtifactType.Coordinator, status=model.CoordinatorStatus.running())
            mock_get.assert_called_with('jobs?jobtype=coordinator&filter=status=RUNNING;status=RUNNINGWITHERROR;'
                                        'status=SUSPENDED;status=SUSPENDEDWITHERROR&offset=1&len=5000')

            api._jobs_query(
                model.ArtifactType.Coordinator,
                user='john_doe',
                name='my_coordinator',
                status=model.CoordinatorStatus.running())
            mock_get.assert_called_with('jobs?jobtype=coordinator&filter=user=john_doe;name=my_coordinator;'
                                        'status=RUNNING;status=RUNNINGWITHERROR;status=SUSPENDED;'
                                        'status=SUSPENDEDWITHERROR&offset=1&len=5000')

    def test_jobs_query_bad_parameters(self, api):
        with pytest.raises(KeyError) as err:
            api._jobs_query(model.ArtifactType.CoordinatorAction)
        assert 'ArtifactType.CoordinatorAction' in str(err)

        with pytest.raises(KeyError) as err:
            api._jobs_query(model.ArtifactType.WorkflowAction)
        assert 'ArtifactType.WorkflowAction' in str(err)

    @mock.patch.object(model.Workflow, 'fill_in_details', side_effect=lambda c: c, autospec=True)
    def test_jobs_query_workflow_pagination(self, _, api):
        mock_results = iter(
            [
                {
                    'total': 5001,
                    'workflows': [{'id': '1-W'}, {'id': '2-W'}]
                },
                {
                    'total': 5001,
                    'workflows': [{'id': '3-W'}]
                }
            ]
        )
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.side_effect = lambda url: next(mock_results)
            result = api._jobs_query(model.ArtifactType.Workflow)
            assert len(result) == 3
            mock_get.assert_any_call('jobs?jobtype=wf&offset=1&len=5000')
            mock_get.assert_any_call('jobs?jobtype=wf&offset=5001&len=5000')
            with pytest.raises(StopIteration):
                next(mock_results)

    @mock.patch.object(model.Coordinator, 'fill_in_details', side_effect=lambda c: c, autospec=True)
    def test_jobs_query_coordinator_pagination(self, _, api):
        mock_results = iter(
            [
                {
                    'total': 5001,
                    'coordinatorjobs': [{'coordJobId': '1-C'}, {'coordJobId': '2-C'}]
                },
                {
                    'total': 5001,
                    'coordinatorjobs': [{'coordJobId': '3-C'}]
                }
            ]
        )

        with mock.patch.object(api, '_get') as mock_get:
            mock_get.side_effect = lambda url: next(mock_results)
            result = api._jobs_query(model.ArtifactType.Coordinator)
            assert len(result) == 3
            mock_get.assert_any_call('jobs?jobtype=coordinator&offset=1&len=5000')
            mock_get.assert_any_call('jobs?jobtype=coordinator&offset=5001&len=5000')
            with pytest.raises(StopIteration):
                next(mock_results)

    @mock.patch.object(model.Coordinator, 'fill_in_details', side_effect=lambda c: c, autospec=True)
    def test_jobs_query_coordinator_limit(self, _, api):
        # mock_result = {'total': 1, 'coordinatorjobs': [{'coordJobId': '3-C'}]}
        mock_results = iter(
            [
                {
                    'total': 2,
                    'coordinatorjobs': [{'coordJobId': '1-C'}, {'coordJobId': '2-C'}]
                },
                {
                    'total': 5001,
                    'coordinatorjobs': [{'coordJobId': '1-C'}, {'coordJobId': '2-C'}]
                },
                {
                    'total': 5001,
                    'coordinatorjobs': [{'coordJobId': '3-C'}]
                }
            ]
        )

        with mock.patch.object(api, '_get') as mock_get:
            mock_get.side_effect = lambda url: next(mock_results)
            api._jobs_query(model.ArtifactType.Coordinator, limit=5)
            mock_get.assert_called_with('jobs?jobtype=coordinator&offset=1&len=5')
            api._jobs_query(model.ArtifactType.Coordinator, limit=6000)
            mock_get.assert_any_call('jobs?jobtype=coordinator&offset=1&len=5000')
            mock_get.assert_any_call('jobs?jobtype=coordinator&offset=5001&len=5000')
            with pytest.raises(StopIteration):
                next(mock_results)


    @mock.patch.object(model.Workflow, 'fill_in_details', side_effect=lambda c: c, autospec=True)
    def test_jobs_query_workflow_details(self, fill_in_details, api):
        mock_result = {
            'total': 1,
            'workflows': [{'id': '1-W'}]
        }
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.return_value = mock_result

            api._jobs_query(model.ArtifactType.Workflow, details=False)
            mock_get.assert_called_with('jobs?jobtype=wf&offset=1&len=5000')
            assert not fill_in_details.called

            api._jobs_query(model.ArtifactType.Workflow, details=True)
            mock_get.assert_called_with('jobs?jobtype=wf&offset=1&len=5000')
            assert fill_in_details.called

    @mock.patch.object(model.Coordinator, 'fill_in_details', side_effect=lambda c: c, autospec=True)
    def test_jobs_query_coordinator_details(self, fill_in_details, api):
        mock_result = {
            'total': 1,
            'coordinatorjobs': [{'coordJobId': '1-C'}]
        }
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.return_value = mock_result

            api._jobs_query(model.ArtifactType.Coordinator, details=False)
            mock_get.assert_called_with('jobs?jobtype=coordinator&offset=1&len=5000')
            assert not fill_in_details.called

            api._jobs_query(model.ArtifactType.Coordinator, details=True)
            mock_get.assert_called_with('jobs?jobtype=coordinator&offset=1&len=5000')
            assert fill_in_details.called

    def test_jobs_all_workflows(self, api, sample_workflow_running):
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_workflow_running]

            api.jobs_all_workflows()
            mock_query.assert_called_with(model.ArtifactType.Workflow, name=None, user=None, limit=0)

            api.jobs_all_workflows(name='my_workflow')
            mock_query.assert_called_with(model.ArtifactType.Workflow, name='my_workflow', user=None, limit=0)

            api.jobs_all_workflows(user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Workflow, name=None, user='john_doe', limit=0)

            api.jobs_all_workflows(name='my_workflow', user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Workflow, name='my_workflow', user='john_doe', limit=0)

            api.jobs_all_workflows(name='my_workflow', limit=10)
            mock_query.assert_called_with(model.ArtifactType.Workflow, name='my_workflow', user=None, limit=10)

    def test_jobs_all_active_workflows(self, api, sample_workflow_running):
        expected_statuses = model.WorkflowStatus.active()
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_workflow_running]

            api.jobs_all_active_workflows()
            mock_query.assert_called_with(model.ArtifactType.Workflow, details=True, user=None, status=expected_statuses)

            api.jobs_all_active_workflows(user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Workflow, details=True, user='john_doe', status=expected_statuses)

    def test_jobs_all_running_workflows(self, api, sample_workflow_running):
        expected_statuses = model.WorkflowStatus.running()
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_workflow_running]

            api.jobs_all_running_workflows()
            mock_query.assert_called_with(model.ArtifactType.Workflow, details=True, user=None, status=expected_statuses)

            api.jobs_all_running_workflows(user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Workflow, details=True, user='john_doe', status=expected_statuses)

    def test_jobs_running_workflows(self, api, sample_workflow_running):
        expected_statuses = model.WorkflowStatus.running()
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_workflow_running]

            api.jobs_running_workflows('my_workflow')
            mock_query.assert_called_with(
                model.ArtifactType.Workflow,
                details=True,
                name='my_workflow',
                user=None,
                status=expected_statuses)

            api.jobs_running_workflows('my_workflow', user='john_doe')
            mock_query.assert_called_with(
                model.ArtifactType.Workflow,
                details=True,
                name='my_workflow',
                user='john_doe',
                status=expected_statuses)

    def test_jobs_last_workflow_parameters(self, api, sample_workflow_running):
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_workflow_running]

            api.jobs_last_workflow('my_workflow')
            mock_query.assert_called_with(model.ArtifactType.Workflow, name='my_workflow', user=None, limit=1)

            api.jobs_last_workflow('my_workflow', user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Workflow, name='my_workflow', user='john_doe', limit=1)

    def test_jobs_workflow_names_parameters(self, api):
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = []

            api.jobs_workflow_names()
            mock_query.assert_called_with(model.ArtifactType.Workflow, user=None, details=False, limit=0)

            api.jobs_workflow_names(user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Workflow, user='john_doe', details=False, limit=0)

    def test_jobs_all_coordinators(self, api, sample_coordinator_running):
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_coordinator_running]

            api.jobs_all_coordinators()
            mock_query.assert_called_with(model.ArtifactType.Coordinator, details=True, name=None, user=None, limit=0)

            api.jobs_all_coordinators(name='my_coordinator')
            mock_query.assert_called_with(model.ArtifactType.Coordinator, details=True, name='my_coordinator', user=None, limit=0)

            api.jobs_all_coordinators(user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Coordinator, details=True, name=None, user='john_doe', limit=0)

            api.jobs_all_coordinators(name='my_coordinator', user='john_doe')
            mock_query.assert_called_with(
                model.ArtifactType.Coordinator,
                details=True,
                name='my_coordinator',
                user='john_doe',
                limit=0)

            api.jobs_all_coordinators(name='my_coordinator', limit=1)
            mock_query.assert_called_with(
                model.ArtifactType.Coordinator,
                details=True,
                name='my_coordinator',
                user=None,
                limit=1)

    def test_jobs_all_active_coordinators(self, api, sample_coordinator_running):
        expected_statuses = model.CoordinatorStatus.active()
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_coordinator_running]

            api.jobs_all_active_coordinators()
            mock_query.assert_called_with(model.ArtifactType.Coordinator, details=True, user=None, status=expected_statuses)

            api.jobs_all_active_coordinators(user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Coordinator, details=True, user='john_doe', status=expected_statuses)

    def test_jobs_all_running_coordinators(self, api, sample_coordinator_running):
        expected_statuses = model.CoordinatorStatus.running()
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_coordinator_running]

            api.jobs_all_running_coordinators()
            mock_query.assert_called_with(model.ArtifactType.Coordinator, details=True, user=None, status=expected_statuses)

            api.jobs_all_running_coordinators(user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Coordinator, details=True, user='john_doe', status=expected_statuses)

    def test_jobs_all_suspended_coordinators(self, api, sample_coordinator_suspended):
        expected_statuses = model.CoordinatorStatus.suspended()
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_coordinator_suspended]

            api.jobs_all_suspended_coordinators()
            mock_query.assert_called_with(model.ArtifactType.Coordinator, user=None, status=expected_statuses)

            api.jobs_all_suspended_coordinators(user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Coordinator, user='john_doe', status=expected_statuses)

    def test_jobs_running_coordinators(self, api, sample_coordinator_running):
        expected_statuses = model.CoordinatorStatus.running()
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_coordinator_running]

            api.jobs_running_coordinators('my_coordinator')
            mock_query.assert_called_with(
                model.ArtifactType.Coordinator,
                name='my_coordinator',
                user=None,
                status=expected_statuses)

            api.jobs_running_coordinators('my_coordinator', user='john_doe')
            mock_query.assert_called_with(
                model.ArtifactType.Coordinator,
                name='my_coordinator',
                user='john_doe',
                status=expected_statuses)

    def test_jobs_last_coordinator_parameters(self, api, sample_coordinator_running):
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = [sample_coordinator_running]

            api.jobs_last_coordinator('my_coordinator')
            mock_query.assert_called_with(model.ArtifactType.Coordinator, name='my_coordinator', user=None, limit=1)

            api.jobs_last_coordinator('my_coordinator', user='john_doe')
            mock_query.assert_called_with(
                model.ArtifactType.Coordinator,
                name='my_coordinator',
                user='john_doe',
                limit=1)

    def test_jobs_coordinator_names_parameters(self, api):
        with mock.patch.object(api, '_jobs_query') as mock_query:
            mock_query.return_value = []

            api.jobs_coordinator_names()
            mock_query.assert_called_with(model.ArtifactType.Coordinator, user=None, details=False)

            api.jobs_coordinator_names(user='john_doe')
            mock_query.assert_called_with(model.ArtifactType.Coordinator, user='john_doe', details=False)


class TestOozieClientJobCoordinatorQuery(object):

    def test_coordinator_query_parameters(self, api):
        mock_coord = {
            'total': 0,
            'coordJobId': SAMPLE_COORD_ID,
            'actions': []
        }
        mock_action = {
            'id': SAMPLE_COORD_ACTION,
        }
        with mock.patch.object(api, '_get') as mock_get:
            def dummy_get(url):
                if url.startswith('job/' + SAMPLE_COORD_ID + '?'):
                    return mock_coord
                elif url.startswith('job/' + SAMPLE_COORD_ID + '@'):
                    return mock_action
                assert False, 'Unexpected URL'
            mock_get.side_effect = dummy_get

            with pytest.raises(ValueError) as err:
                api._coordinator_query('foo')
            assert 'Unrecognized job ID' in str(err)
            assert not mock_get.called

            api._coordinator_query(SAMPLE_COORD_ID)
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?offset=1&len=1')
            mock_get.reset_mock()

            with pytest.raises(ValueError) as err:
                api._coordinator_query(SAMPLE_COORD_ID + '@foo')
            assert 'Unrecognized job ID' in str(err)
            assert not mock_get.called

            api._coordinator_query(SAMPLE_COORD_ACTION)
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?offset=12&len=1')
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ACTION)
            mock_get.reset_mock()

            api._coordinator_query(SAMPLE_COORD_ID, status=model.CoordinatorActionStatus.RUNNING)
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?offset=1&len=1&filter=status=RUNNING')
            mock_get.reset_mock()

            api._coordinator_query(SAMPLE_COORD_ID, status=model.CoordinatorActionStatus.running())
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID +
                                     '?offset=1&len=1&filter=status=RUNNING;status=SUSPENDED')
            mock_get.reset_mock()

            with pytest.raises(ValueError) as err:
                api._coordinator_query(SAMPLE_COORD_ACTION, status=model.CoordinatorActionStatus.RUNNING)
            assert 'Cannot supply both coordinator action ID and status' in str(err)
            assert not mock_get.called

    def test_coordinator_query_limits(self, api):
        mock_result = {
            'total': 100,
            'coordJobId': SAMPLE_COORD_ID,
            'actions': []
        }
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.return_value = mock_result

            with pytest.raises(ValueError) as err:
                api._coordinator_query(SAMPLE_COORD_ACTION, start=1)
            assert 'Cannot supply both coordinator action ID and start / limit' in str(err)

            with pytest.raises(ValueError) as err:
                api._coordinator_query(SAMPLE_COORD_ACTION, limit=10)
            assert 'Cannot supply both coordinator action ID and start / limit' in str(err)

            api._coordinator_query(SAMPLE_COORD_ID)
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?offset=1&len=1')
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?offset=1&len=100')

            api._coordinator_query(SAMPLE_COORD_ID, start=10)
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?offset=10&len=1')
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?offset=10&len=91')

            api._coordinator_query(SAMPLE_COORD_ID, limit=10)
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?order=desc&offset=1&len=10')

            api._coordinator_query(SAMPLE_COORD_ID, start=10, limit=10)
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?offset=10&len=10')

            api._coordinator_query(SAMPLE_COORD_ID, start=99, limit=10)
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?offset=99&len=10')

            api._coordinator_query(SAMPLE_COORD_ID, status=model.CoordinatorActionStatus.RUNNING, start=10, limit=10)
            mock_get.assert_any_call('job/' + SAMPLE_COORD_ID + '?offset=10&len=10&filter=status=RUNNING')

    def test_coordinator_query_exception(self, api):
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.side_effect = exceptions.OozieException.communication_error('A bad thing')

            with pytest.raises(exceptions.OozieException) as err:
                api._coordinator_query(SAMPLE_COORD_ID)
            assert "Coordinator '" + SAMPLE_COORD_ID + "' not found" in str(err)
            assert 'A bad thing' in str(err.value.caused_by)

    def test_coordinator_action_query(self, api):
        mock_result = {
            'id': SAMPLE_COORD_ACTION,
        }
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.return_value = mock_result
            mock_coord = mock.Mock()
            mock_coord.actions = {}
            action = api._coordinator_action_query(SAMPLE_COORD_ID, 12, coordinator=mock_coord)
            mock_get.assert_called_with('job/' + SAMPLE_COORD_ACTION)
            assert action._parent == mock_coord

    def test_coordinator_action_query_exception(self, api):
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.side_effect = exceptions.OozieException.communication_error('A bad thing')

            with pytest.raises(exceptions.OozieException) as err:
                api._coordinator_action_query(SAMPLE_COORD_ID, 12)
            assert "Coordinator action '" + SAMPLE_COORD_ID + "@12' not found" in str(err)
            assert 'A bad thing' in str(err.value.caused_by)

    def test_decode_coord_id(self, api, sample_coordinator_running):
        with mock.patch.object(api, 'jobs_last_coordinator') as mock_last:
            mock_last.return_value = mock.Mock(coordJobId=SAMPLE_COORD_ID)

            with pytest.raises(ValueError) as err:
                api._decode_coord_id()
            assert 'Supply exactly one of coordinator_id or name' in str(err)

            with pytest.raises(ValueError) as err:
                api._decode_coord_id(coordinator_id=SAMPLE_COORD_ID, name='my_coordinator')
            assert 'Supply exactly one of coordinator_id or name' in str(err)

            with pytest.raises(ValueError) as err:
                api._decode_coord_id(coordinator_id=SAMPLE_COORD_ID, user='john_doe')
            assert 'User parameter not supported with coordinator_id' in str(err)

            result = api._decode_coord_id(coordinator_id=SAMPLE_COORD_ID)
            assert result == SAMPLE_COORD_ID

            result = api._decode_coord_id(name='my_coordinator')
            assert result == SAMPLE_COORD_ID
            mock_last.assert_called_with(name='my_coordinator', user=None)

            result = api._decode_coord_id(name='my_coordinator', user='john_doe')
            assert result == SAMPLE_COORD_ID
            mock_last.assert_called_with(name='my_coordinator', user='john_doe')

            mock_last.return_value = None
            with pytest.raises(exceptions.OozieException) as err:
                api._decode_coord_id(name='my_coordinator')
            assert "Coordinator 'my_coordinator' not found" in str(err)

            result = api._decode_coord_id(coordinator=sample_coordinator_running)
            assert result == SAMPLE_COORD_ID

            with pytest.raises(ValueError) as err:
                api._decode_coord_id(coordinator_id=SAMPLE_COORD_ID, coordinator=sample_coordinator_running)
            assert 'Supply either a coordinator object or one of coordinator_id or name' in str(err)

            with pytest.raises(ValueError) as err:
                api._decode_coord_id(name='my_coordinator', coordinator=sample_coordinator_running)
            assert 'Supply either a coordinator object or one of coordinator_id or name' in str(err)

            with pytest.raises(ValueError) as err:
                api._decode_coord_id(coordinator=sample_coordinator_running, user='john_doe')
            assert 'User parameter not supported with coordinator object' in str(err)

    def test_job_coordinator_info(self, api):
        with mock.patch.object(api, '_coordinator_query') as mock_query:
            with mock.patch.object(api, '_decode_coord_id') as mock_decode:
                mock_decode.return_value = SAMPLE_COORD_ID

                api.job_coordinator_info(coordinator_id=SAMPLE_COORD_ID)
                mock_decode.assert_called_with(SAMPLE_COORD_ID, None, None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, limit=0)

                api.job_coordinator_info(name='my_coordinator')
                mock_decode.assert_called_with(None, 'my_coordinator', None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, limit=0)

                api.job_coordinator_info(name='my_coordinator', user='john_doe')
                mock_decode.assert_called_with(None, 'my_coordinator', 'john_doe')
                mock_query.assert_called_with(SAMPLE_COORD_ID, limit=0)

                api.job_coordinator_info(coordinator_id=SAMPLE_COORD_ID, limit=10)
                mock_decode.assert_called_with(SAMPLE_COORD_ID, None, None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, limit=10)

    def test_job_last_coordinator_info(self, api):
        with mock.patch.object(api, '_coordinator_query') as mock_query:
            with mock.patch.object(api, '_decode_coord_id') as mock_decode:
                mock_decode.return_value = SAMPLE_COORD_ID

                api.job_last_coordinator_info(coordinator_id=SAMPLE_COORD_ID)
                mock_decode.assert_called_with(SAMPLE_COORD_ID, None, None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, limit=1)

                api.job_last_coordinator_info(name='my_coordinator')
                mock_decode.assert_called_with(None, 'my_coordinator', None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, limit=1)

                api.job_last_coordinator_info(name='my_coordinator', user='john_doe')
                mock_decode.assert_called_with(None, 'my_coordinator', 'john_doe')
                mock_query.assert_called_with(SAMPLE_COORD_ID, limit=1)

    def test_job_coordinator_action(self, api):
        with mock.patch.object(api, '_coordinator_action_query') as mock_query:
            with mock.patch.object(api, '_decode_coord_id') as mock_decode:
                mock_decode.return_value = SAMPLE_COORD_ID

                api.job_coordinator_action(SAMPLE_COORD_ACTION)
                mock_decode.assert_called_with(SAMPLE_COORD_ACTION, None, None, None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, 12, coordinator=None)

                api.job_coordinator_action(SAMPLE_COORD_ID, action_number=12)
                mock_decode.assert_called_with(SAMPLE_COORD_ID, None, None, None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, 12, coordinator=None)

                api.job_coordinator_action(name='my_coordinator', action_number=12)
                mock_decode.assert_called_with(None, 'my_coordinator', None, None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, 12, coordinator=None)

                api.job_coordinator_action(name='my_coordinator', user='john_doe', action_number=12)
                mock_decode.assert_called_with(None, 'my_coordinator', 'john_doe', None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, 12, coordinator=None)

                with pytest.raises(ValueError) as err:
                    api.job_coordinator_action(SAMPLE_COORD_ACTION, action_number=12)
                assert 'Supply exactly one of coordinator_id or action_number' in str(err)

                with pytest.raises(ValueError) as err:
                    api.job_coordinator_action(name='my_coordinator')
                assert 'No action_number supplied' in str(err)

    def test_job_coordinator_all_active_actions(self, api, sample_coordinator_running,
                                                sample_coordinator_action_running):
        with mock.patch.object(api, '_coordinator_query') as mock_query:
            mock_query.return_value = sample_coordinator_action_running.parent()
            with mock.patch.object(api, '_decode_coord_id') as mock_decode:
                mock_decode.return_value = SAMPLE_COORD_ID

                api.job_coordinator_all_active_actions(coordinator_id=SAMPLE_COORD_ID)
                mock_decode.assert_called_with(SAMPLE_COORD_ID, None, None, None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, status=model.CoordinatorActionStatus.active())

                api.job_coordinator_all_active_actions(name='my_coordinator')
                mock_decode.assert_called_with(None, 'my_coordinator', None, None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, status=model.CoordinatorActionStatus.active())

                api.job_coordinator_all_active_actions(name='my_coordinator', user='john_doe')
                mock_decode.assert_called_with(None, 'my_coordinator', 'john_doe', None)
                mock_query.assert_called_with(SAMPLE_COORD_ID, status=model.CoordinatorActionStatus.active())

                sample_coordinator = copy.copy(sample_coordinator_running)
                sample_coordinator.actions = None
                api.job_coordinator_all_active_actions(coordinator=sample_coordinator)
                mock_decode.assert_called_with(None, None, None, sample_coordinator)
                mock_query.assert_called_with(SAMPLE_COORD_ID, status=model.CoordinatorActionStatus.active())
                assert sample_coordinator.actions
                assert sample_coordinator.actions[12] == sample_coordinator_action_running


class TestOozieClientJobWorkflowQuery(object):

    def test_workflow_query_parameters(self, api):
        mock_result = {
            'total': 0,
            'id': SAMPLE_WF_ID,
            'actions': []
        }
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.return_value = mock_result

            with pytest.raises(ValueError) as err:
                api._workflow_query('foo')
            assert 'Unrecognized job ID' in str(err)

            api._workflow_query(SAMPLE_WF_ID)
            mock_get.assert_called_with('job/' + SAMPLE_WF_ID)

            api._workflow_query(SAMPLE_WF_ACTION)
            mock_get.assert_called_with('job/' + SAMPLE_WF_ID)

    def test_workflow_query_exception(self, api):
        with mock.patch.object(api, '_get') as mock_get:
            mock_get.side_effect = exceptions.OozieException.communication_error('A bad thing')

            with pytest.raises(exceptions.OozieException) as err:
                api._workflow_query(SAMPLE_WF_ID)
            assert "Workflow '" + SAMPLE_WF_ID + "' not found" in str(err)
            assert 'A bad thing' in str(err.value.caused_by)

    def test_decode_wf_id(self, api):
        with mock.patch.object(api, 'jobs_last_workflow') as mock_last:
            mock_last.return_value = mock.Mock(id=SAMPLE_WF_ID)

            with pytest.raises(ValueError) as err:
                api._decode_wf_id()
            assert 'Supply exactly one of workflow_id or name' in str(err)

            with pytest.raises(ValueError) as err:
                api._decode_wf_id(workflow_id=SAMPLE_WF_ID, name='my_workflow')
            assert 'Supply exactly one of workflow_id or name' in str(err)

            with pytest.raises(ValueError) as err:
                api._decode_wf_id(workflow_id=SAMPLE_WF_ID, user='john_doe')
            assert 'User parameter not supported with workflow_id' in str(err)

            result = api._decode_wf_id(workflow_id=SAMPLE_WF_ID)
            assert result == SAMPLE_WF_ID

            result = api._decode_wf_id(name='my_workflow')
            assert result == SAMPLE_WF_ID
            mock_last.assert_called_with(name='my_workflow', user=None)

            result = api._decode_wf_id(name='my_workflow', user='john_doe')
            assert result == SAMPLE_WF_ID
            mock_last.assert_called_with(name='my_workflow', user='john_doe')

            mock_last.return_value = None
            with pytest.raises(exceptions.OozieException) as err:
                api._decode_wf_id(name='my_workflow')
            assert "Workflow 'my_workflow' not found" in str(err)

    def test_job_workflow_info(self, api):
        with mock.patch.object(api, '_workflow_query') as mock_query:
            with mock.patch.object(api, '_decode_wf_id') as mock_decode:
                mock_decode.return_value = SAMPLE_WF_ID

                api.job_workflow_info(workflow_id=SAMPLE_WF_ID)
                mock_decode.assert_called_with(SAMPLE_WF_ID, None, None)
                mock_query.assert_called_with(SAMPLE_WF_ID)

                api.job_workflow_info(name='my_workflow')
                mock_decode.assert_called_with(None, 'my_workflow', None)
                mock_query.assert_called_with(SAMPLE_WF_ID)

                api.job_workflow_info(name='my_workflow', user='john_doe')
                mock_decode.assert_called_with(None, 'my_workflow', 'john_doe')
                mock_query.assert_called_with(SAMPLE_WF_ID)


class TestOozieClientJobQuery(object):

    def test_job_info(self, api):
        with mock.patch.object(api, 'job_coordinator_info') as mock_coord_info:
            with mock.patch.object(api, 'job_workflow_info') as mock_workflow_info:
                api.job_info(SAMPLE_COORD_ID)
                mock_coord_info.assert_called_with(coordinator_id=SAMPLE_COORD_ID)
                assert not mock_workflow_info.called
                mock_coord_info.reset_mock()

                api.job_info(SAMPLE_COORD_ACTION)
                mock_coord_info.assert_called_with(coordinator_id=SAMPLE_COORD_ACTION)
                assert not mock_workflow_info.called
                mock_coord_info.reset_mock()

                api.job_info(SAMPLE_WF_ID)
                mock_workflow_info.assert_called_with(workflow_id=SAMPLE_WF_ID)
                assert not mock_coord_info.called
                mock_workflow_info.reset_mock()

                api.job_info(SAMPLE_WF_ACTION)
                mock_workflow_info.assert_called_with(workflow_id=SAMPLE_WF_ACTION)
                assert not mock_coord_info.called
                mock_workflow_info.reset_mock()

                with pytest.raises(exceptions.OozieException) as err:
                    api.job_info("wat?")
                assert "'wat?' does not match any known job" in str(err)
                assert not mock_coord_info.called
                assert not mock_workflow_info.called

    def test_job_action_info(self, api):
        with mock.patch.object(api, 'job_coordinator_info') as mock_coord_info:
            with mock.patch.object(api, 'job_workflow_info') as mock_workflow_info:
                api.job_action_info(SAMPLE_COORD_ID)
                mock_coord_info.assert_called_with(coordinator_id=SAMPLE_COORD_ID)
                assert not mock_coord_info.action.called
                assert not mock_workflow_info.called
                mock_coord_info.reset_mock()

                api.job_action_info(SAMPLE_COORD_ACTION)
                mock_coord_info.assert_called_with(coordinator_id=SAMPLE_COORD_ACTION)
                mock_coord_info().action.assert_called_with(12)
                assert not mock_workflow_info.called
                mock_coord_info.reset_mock()

                api.job_action_info(SAMPLE_WF_ID)
                mock_workflow_info.assert_called_with(workflow_id=SAMPLE_WF_ID)
                assert not mock_workflow_info.action.called
                assert not mock_coord_info.called
                mock_workflow_info.reset_mock()

                api.job_action_info(SAMPLE_WF_ACTION)
                mock_workflow_info.assert_called_with(workflow_id=SAMPLE_WF_ACTION)
                mock_workflow_info().action.assert_called_with('foo')
                assert not mock_coord_info.called
                mock_workflow_info.reset_mock()

                with pytest.raises(exceptions.OozieException) as err:
                    api.job_action_info("wat?")
                assert "'wat?' does not match any known job" in str(err)
                assert not mock_coord_info.called
                assert not mock_workflow_info.called


class TestOozieClientJobCoordinatorManage(object):

    def test_fetch_coordinator_or_action(self, api, sample_coordinator_running, sample_coordinator_action_running):
        with mock.patch.object(api, '_decode_coord_id') as mock_decode:
            with mock.patch.object(api, 'job_coordinator_info') as mock_info:
                mock_decode.return_value = SAMPLE_COORD_ID
                mock_info.return_value = sample_coordinator_running
                result = api._fetch_coordinator_or_action(SAMPLE_COORD_ID)
                assert result == sample_coordinator_running
                assert mock_decode.called
                assert mock_info.called

        with mock.patch.object(api, '_decode_coord_id') as mock_decode:
            with mock.patch.object(api, 'job_coordinator_info') as mock_info:
                mock_decode.return_value = SAMPLE_COORD_ACTION
                mock_info.return_value = sample_coordinator_action_running.coordinator()
                result = api._fetch_coordinator_or_action(SAMPLE_COORD_ACTION)
                assert result == sample_coordinator_action_running
                assert mock_decode.called
                assert mock_info.called

    def test_job_coordinator_suspend_coordinator(self, api, sample_coordinator_running, sample_coordinator_suspended):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_action_info') as mock_info:
                mock_info.return_value = sample_coordinator_running
                assert api.job_coordinator_suspend(SAMPLE_COORD_ID)
                mock_put.assert_called_with('job/' + SAMPLE_COORD_ID + '?action=suspend')
                mock_put.reset_mock()

                mock_info.return_value = sample_coordinator_suspended
                assert not api.job_coordinator_suspend(SAMPLE_COORD_ID)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_coordinator_suspend_coordinator_action(self, api, sample_coordinator_action_running,
                                                        sample_coordinator_action_suspended):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_action_info') as mock_info:
                mock_info.return_value = sample_coordinator_action_running
                assert api.job_coordinator_suspend(SAMPLE_COORD_ACTION)
                mock_put.assert_called_with('job/' + SAMPLE_COORD_ID + '?action=suspend&type=action&scope=12')
                mock_put.reset_mock()

                mock_info.return_value = sample_coordinator_action_suspended
                assert not api.job_coordinator_suspend(SAMPLE_COORD_ACTION)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_coordinator_resume_coordinator(self, api, sample_coordinator_running, sample_coordinator_suspended):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_action_info') as mock_info:
                mock_info.return_value = sample_coordinator_suspended
                assert api.job_coordinator_resume(SAMPLE_COORD_ID)
                mock_put.assert_called_with('job/' + SAMPLE_COORD_ID + '?action=resume')
                mock_put.reset_mock()

                mock_info.return_value = sample_coordinator_running
                assert not api.job_coordinator_resume(SAMPLE_COORD_ID)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_coordinator_resume_coordinator_action(self, api, sample_coordinator_action_running,
                                                       sample_coordinator_action_suspended):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_action_info') as mock_info:
                mock_info.return_value = sample_coordinator_action_suspended
                assert api.job_coordinator_resume(SAMPLE_COORD_ACTION)
                mock_put.assert_called_with('job/' + SAMPLE_COORD_ID + '?action=resume&type=action&scope=12')
                mock_put.reset_mock()

                mock_info.return_value = sample_coordinator_action_running
                assert not api.job_coordinator_resume(SAMPLE_COORD_ACTION)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_coordinator_kill_coordinator(self, api, sample_coordinator_running, sample_coordinator_killed):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_action_info') as mock_info:
                mock_info.return_value = sample_coordinator_running
                assert api.job_coordinator_kill(SAMPLE_COORD_ID)
                mock_put.assert_called_with('job/' + SAMPLE_COORD_ID + '?action=kill')
                mock_put.reset_mock()

                mock_info.return_value = sample_coordinator_killed
                assert not api.job_coordinator_kill(SAMPLE_COORD_ID)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_coordinator_kill_coordinator_action(self, api, sample_coordinator_action_running,
                                                     sample_coordinator_action_killed):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_action_info') as mock_info:
                mock_info.return_value = sample_coordinator_action_running
                assert api.job_coordinator_kill(SAMPLE_COORD_ACTION)
                mock_put.assert_called_with('job/' + SAMPLE_COORD_ID + '?action=kill&type=action&scope=12')
                mock_put.reset_mock()

                mock_info.return_value = sample_coordinator_action_killed
                assert not api.job_coordinator_kill(SAMPLE_COORD_ACTION)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_coordinator_rerun(self, api, sample_coordinator_action_running,
                                   sample_coordinator_action_killed,
                                   sample_coordinator_action_killed_with_killed_coordinator):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_action_info') as mock_info:
                mock_info.return_value = sample_coordinator_action_killed
                assert api.job_coordinator_rerun(SAMPLE_COORD_ACTION)
                mock_put.assert_called_with('job/' + SAMPLE_COORD_ID +
                                            '?action=coord-rerun&type=action&scope=12&refresh=true')
                mock_put.reset_mock()

                mock_info.return_value = sample_coordinator_action_killed_with_killed_coordinator
                assert not api.job_coordinator_rerun(SAMPLE_COORD_ACTION)
                assert not mock_put.called
                mock_put.reset_mock()

                mock_info.return_value = sample_coordinator_action_running
                assert not api.job_coordinator_rerun(SAMPLE_COORD_ACTION)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_coordinator_rerun_only_supports_actions(self, api, sample_coordinator_running):
        with mock.patch.object(api, 'job_action_info') as mock_info:
            mock_info.return_value = sample_coordinator_running
            with pytest.raises(ValueError) as value_error:
                api.job_coordinator_rerun(SAMPLE_COORD_ID)
            assert str(value_error.value) == 'Rerun only supports coordinator action IDs'

    def test_job_coordinator_update(self, api, sample_coordinator_running, sample_coordinator_killed):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_coordinator_info') as mock_info:
                mock_info.return_value = sample_coordinator_running

                mock_put.return_value = {'update': {'diff': "****Empty Diff****"}}

                coord = api.job_coordinator_update(SAMPLE_COORD_ID, '/dummy/coord-path-minimal')

                conf = xml._coordinator_submission_xml('oozie', '/dummy/coord-path-minimal')
                mock_put.assert_called_with('job/' + SAMPLE_COORD_ID + "?action=update", conf)
                mock_info.assert_called_with(coordinator_id=SAMPLE_COORD_ID)
                assert coord is sample_coordinator_running

                mock_put.reset_mock()
                mock_info.reset_mock()

                mock_info.return_value = sample_coordinator_running
                mock_put.return_value = {'update': {'diff': "*****Diffs*****"}}

                coord = api.job_coordinator_update(SAMPLE_COORD_ID, '/dummy/coord-path-full')

                conf = xml._coordinator_submission_xml('oozie', '/dummy/coord-path-full')
                mock_put.assert_called_with('job/' + SAMPLE_COORD_ID + "?action=update", conf)
                mock_info.assert_called_with(coordinator_id=SAMPLE_COORD_ID)
                assert coord is sample_coordinator_running

                mock_put.reset_mock()
                mock_info.reset_mock()

                mock_info.return_value = sample_coordinator_killed

                with pytest.raises(exceptions.OozieException) as err:
                    api.job_coordinator_update(SAMPLE_COORD_ID, '/dummy/coord-path-full')

                assert 'coordinator status must be active in order to update' in str(err)

                mock_info.return_value = sample_coordinator_running
                mock_put.return_value = {}
                with pytest.raises(exceptions.OozieException) as err:
                    api.job_coordinator_update(SAMPLE_COORD_ID, '/dummy/coord-path-full')

                assert 'update coordinator' in str(err)


class TestOozieClientJobWorkflowManage(object):

    def test_job_workflow_suspend_workflow(self, api, sample_workflow_running, sample_workflow_suspended):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_workflow_info') as mock_info:
                mock_info.return_value = sample_workflow_running
                assert api.job_workflow_suspend(SAMPLE_WF_ID)
                mock_put.assert_called_with('job/' + SAMPLE_WF_ID + '?action=suspend')
                mock_put.reset_mock()

                mock_info.return_value = sample_workflow_suspended
                assert not api.job_workflow_suspend(SAMPLE_WF_ID)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_workflow_suspend_workflow_action(self, api, sample_workflow_running, sample_workflow_suspended):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_workflow_info') as mock_info:
                mock_info.return_value = sample_workflow_running
                assert api.job_workflow_suspend(SAMPLE_WF_ACTION)
                mock_put.assert_called_with('job/' + SAMPLE_WF_ID + '?action=suspend')
                mock_put.reset_mock()

                mock_info.return_value = sample_workflow_suspended
                assert not api.job_workflow_suspend(SAMPLE_WF_ACTION)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_workflow_resume_workflow(self, api, sample_workflow_running, sample_workflow_suspended):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_workflow_info') as mock_info:
                mock_info.return_value = sample_workflow_suspended
                assert api.job_workflow_resume(SAMPLE_WF_ID)
                mock_put.assert_called_with('job/' + SAMPLE_WF_ID + '?action=resume')
                mock_put.reset_mock()

                mock_info.return_value = sample_workflow_running
                assert not api.job_workflow_resume(SAMPLE_WF_ID)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_workflow_resume_workflow_action(self, api, sample_workflow_running, sample_workflow_suspended):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_workflow_info') as mock_info:
                mock_info.return_value = sample_workflow_suspended
                assert api.job_workflow_resume(SAMPLE_WF_ACTION)
                mock_put.assert_called_with('job/' + SAMPLE_WF_ID + '?action=resume')
                mock_put.reset_mock()

                mock_info.return_value = sample_workflow_running
                assert not api.job_workflow_resume(SAMPLE_WF_ACTION)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_workflow_start_workflow(self, api, sample_workflow_running, sample_workflow_prep):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_workflow_info') as mock_info:
                mock_info.return_value = sample_workflow_prep
                assert api.job_workflow_start(SAMPLE_WF_ID)
                mock_put.assert_called_with('job/' + SAMPLE_WF_ID + '?action=start')
                mock_put.reset_mock()

                mock_info.return_value = sample_workflow_running
                assert not api.job_workflow_start(SAMPLE_WF_ID)
                assert not mock_put.called
                mock_put.reset_mock()

    def test_job_workflow_start_workflow_action(self, api, sample_workflow_running, sample_workflow_prep):
        with mock.patch.object(api, '_put') as mock_put:
            with mock.patch.object(api, 'job_workflow_info') as mock_info:
                mock_info.return_value = sample_workflow_prep
                assert api.job_workflow_start(SAMPLE_WF_ACTION)
                mock_put.assert_called_with('job/' + SAMPLE_WF_ID + '?action=start')
                mock_put.reset_mock()

                mock_info.return_value = sample_workflow_running
                assert not api.job_workflow_start(SAMPLE_WF_ACTION)
                assert not mock_put.called
                mock_put.reset_mock()


class TestOozieClientJobSubmit(object):

    def test_jobs_submit_coordinator(self, api, sample_coordinator_running):
        with mock.patch.object(api, '_post') as mock_post:
            with mock.patch.object(api, 'job_coordinator_info') as mock_info:
                mock_info.return_value = sample_coordinator_running

                mock_post.return_value = None
                with pytest.raises(exceptions.OozieException) as err:
                    api.jobs_submit_coordinator('/dummy/coord-path')
                assert 'Operation failed: submit coordinator' in str(err)
                mock_post.assert_called_with('jobs', mock.ANY)
                mock_post.reset_mock()

                mock_post.return_value = {'id': SAMPLE_COORD_ID}
                coord = api.jobs_submit_coordinator('/dummy/coord-path')
                mock_post.assert_called_with('jobs', mock.ANY)
                mock_info.assert_called_with(coordinator_id=SAMPLE_COORD_ID)
                assert coord is sample_coordinator_running
                mock_post.reset_mock()

    def test_jobs_submit_coordinator_config(self, api, sample_coordinator_running):
        with mock.patch.object(api, '_post') as mock_post:
            with mock.patch.object(api, 'job_coordinator_info') as mock_info:
                mock_info.return_value = sample_coordinator_running
                mock_post.return_value = {'id': SAMPLE_COORD_ID}

                api.jobs_submit_coordinator('/dummy/coord-path')
                conf = mock_post.call_args[0][1].decode('utf-8')
                assert '<name>oozie.coord.application.path</name><value>/dummy/coord-path</value>' in conf
                assert '<name>user.name</name><value>oozie</value>' in conf
                mock_post.reset_mock()

                api.jobs_submit_coordinator('/dummy/coord-path', configuration={'test.prop': 'this is a test'})
                conf = mock_post.call_args[0][1].decode('utf-8')
                assert '<name>test.prop</name><value>this is a test</value>' in conf
                mock_post.reset_mock()

    def test_jobs_submit_workflow(self, api, sample_workflow_running):
        with mock.patch.object(api, '_post') as mock_post:
            with mock.patch.object(api, 'job_workflow_info') as mock_info:
                mock_info.return_value = sample_workflow_running

                mock_post.return_value = None
                with pytest.raises(exceptions.OozieException) as err:
                    api.jobs_submit_workflow('/dummy/wf-path')
                assert 'Operation failed: submit workflow' in str(err)
                mock_post.assert_called_with('jobs', mock.ANY)
                mock_post.reset_mock()

                mock_post.return_value = {'id': SAMPLE_WF_ID}
                wf = api.jobs_submit_workflow('/dummy/wf-path', start=True)
                mock_post.assert_called_with('jobs?action=start', mock.ANY)
                assert wf is sample_workflow_running
                mock_post.reset_mock()

                mock_post.return_value = {'id': SAMPLE_WF_ID}
                wf = api.jobs_submit_workflow('/dummy/wf-path')
                mock_post.assert_called_with('jobs', mock.ANY)
                mock_info.assert_called_with(workflow_id=SAMPLE_WF_ID)
                assert wf is sample_workflow_running
                mock_post.reset_mock()

    def test_jobs_submit_workflow_config(self, api, sample_workflow_running):
        with mock.patch.object(api, '_post') as mock_post:
            with mock.patch.object(api, 'job_workflow_info') as mock_info:
                mock_info.return_value = sample_workflow_running
                mock_post.return_value = {'id': SAMPLE_WF_ID}

                api.jobs_submit_workflow('/dummy/wf-path')
                conf = mock_post.call_args[0][1].decode('utf-8')
                assert '<name>oozie.wf.application.path</name><value>/dummy/wf-path</value>' in conf
                assert '<name>user.name</name><value>oozie</value>' in conf
                mock_post.reset_mock()

                api.jobs_submit_workflow('/dummy/wf-path', configuration={'test.prop': 'this is a test'})
                conf = mock_post.call_args[0][1].decode('utf-8')
                assert '<name>test.prop</name><value>this is a test</value>' in conf
                mock_post.reset_mock()
