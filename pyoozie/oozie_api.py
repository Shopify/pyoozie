from __future__ import unicode_literals

import logging
import requests
import simplejson as json

from starscream.scheduling.oozie.exceptions import OozieException
from starscream.scheduling.oozie.model import ArtifactType, Coordinator, CoordinatorAction, \
    Workflow, WorkflowAction, parse_coordinator_id, parse_workflow_id
from starscream.scheduling.oozie.message import generate_coordinator_submission_xml, \
    generate_workflow_submission_xml
import starscream.log

logger = starscream.log.logger('Oozie API')
logging.getLogger('requests').setLevel(logging.WARNING)


class OozieAPI(object):

    JOB_TYPE_STRINGS = {
        ArtifactType.Coordinator: ('coordinator', 'coordinatorjobs'),
        ArtifactType.Workflow: ('wf', 'workflows'),
    }

    JOB_TYPES = {
        ArtifactType.Coordinator: Coordinator,
        ArtifactType.CoordinatorAction: CoordinatorAction,
        ArtifactType.Workflow: Workflow,
        ArtifactType.WorkflowAction: WorkflowAction,
    }

    STATUS_TYPES = {
        ArtifactType.Coordinator: Coordinator.Status,
        ArtifactType.CoordinatorAction: CoordinatorAction.Status,
        ArtifactType.Workflow: Workflow.Status,
        ArtifactType.WorkflowAction: WorkflowAction.Status,
    }

    class Stats(object):

        def __init__(self):
            self.reset()

        def reset(self):
            self._requests = 0
            self._errors = 0
            self._bytes_received = 0
            self._elapsed = 0

        def update(self, response):
            self._requests += 1
            if response is not None:
                if not response:
                    self._errors += 1
                self._bytes_received += len(response.text)
                self._elapsed += response.elapsed.microseconds
            else:
                self._errors += 1

    def __init__(self, url=None, user=None, timeout=None, verbose=True, **kwargs):
        oozie_url = (url or 'http://localhost').rstrip('/')
        if not oozie_url.endswith('/oozie'):
            oozie_url += '/oozie'
        self._url = oozie_url
        self._user = user
        self._timeout = timeout or 30
        self._verbose = verbose  # Note: change default for verbose!
        self._stats = OozieAPI.Stats()
        self._test_connection()

    def _test_connection(self):
        response = None
        try:
            response = requests.get('{}/versions'.format(self._url), timeout=self._timeout)
            response.raise_for_status()
            self._stats.update(response)
        except requests.RequestException as err:
            self._stats.update(response)
            if self._verbose and response is not None:
                logger.error(response.headers)
            raise OozieException.communication_error("Unable to contact Oozie REST server at {}".format(self._url), err)
        versions = json.loads(response.content)
        if 2 not in versions:
            raise OozieException.communication_error("Oozie REST server at {} does not support API version 2 (supported: {})".format(self._url, versions))

    def _headers(self, content_type=None):
        headers = {}
        if content_type:
            headers['Content-Type'] = content_type
        return headers

    def _request(self, method, endpoint, content_type, content=None):
        response = None
        try:
            url = '{}/v2/{}'.format(self._url, endpoint)
            if self._verbose:
                if content:
                    logger.info("Request: {} {} content bytes: {}".format(method, url, len(content)))
                else:
                    logger.info("Request: {} {}".format(method, url))
            response = requests.request(method, url, data=content, timeout=self._timeout, headers=self._headers(content_type))
            response.raise_for_status()
            self._stats.update(response)
            if self._verbose:
                logger.info("Reply: status={} bytes={} elapsed={}ms".format(response.status_code, len(response.text), response.elapsed.microseconds / 1000.0))
        except requests.RequestException as err:
            self._stats.update(response)
            if self._verbose and response is not None:
                logger.error("Reply: status={} reason={} elapsed={}ms".format(response.status_code, response.reason, response.elapsed.microseconds / 1000.0))
            raise OozieException.communication_error(caused_by=err)
        return json.loads(response.content) if response.content else None

    def _get(self, endpoint, content_type=None):
        return self._request('GET', endpoint, content_type)

    def _put(self, endpoint, content_type='application/xml'):
        return self._request('PUT', endpoint, content_type)

    def _post(self, endpoint, content, content_type='application/xml'):
        return self._request('POST', endpoint, content_type, content)

    def report_stats(self, to_logger=None):
        if not to_logger:
            to_logger = logger
        to_logger.info("OozieAPI Stats: requests={} errors={} bytes={} elapsed={}ms".format(self._stats._requests, self._stats._errors, self._stats._bytes_received, self._stats._elapsed / 1000))

    def reset_stats(self):
        self._stats.reset()

    """
    ===========================================================================
    Admin API
    ===========================================================================
    """

    def _admin_query(self, endpoint):
        return self._get('admin/' + endpoint)

    def admin_status(self):
        return self._admin_query('status')

    def admin_os_env(self):
        return self._admin_query('os-env')

    def admin_java_properties(self):
        return self._admin_query('java-sys-properties')

    def admin_configuration(self):
        return self._admin_query('configuration')

    def admin_instrumentation(self):
        return self._admin_query('instrumentation')

    def admin_metrics(self):
        return self._admin_query('metrics')

    def admin_build_version(self):
        return self._admin_query('build-version')

    def admin_available_timezones(self):
        return self._admin_query('available-timezones')

    def admin_queue_dump(self):
        return self._admin_query('queue-dump')

    def admin_available_oozie_servers(self):
        return self._admin_query('available-oozie-servers')

    def admin_list_sharelib(self):
        return [lib['name'] for lib in self._admin_query('list_sharelib')['sharelib']]

    def admin_list_all_sharelib(self):
        all_libs = dict()
        for lib in self.admin_list_sharelib():
            files = self._admin_query('list_sharelib?lib={}'.format(lib))['sharelib'][0]['files']
            all_libs[lib] = files
        return all_libs

    """
    ===========================================================================
    Jobs API - query coordinators and workflows
    ===========================================================================
    """

    def _jobs_query(self, type_enum, user=None, name=None, status=None, limit=0, details=True):
        job_type, result_type = self.JOB_TYPE_STRINGS[type_enum]
        status_type = self.STATUS_TYPES[type_enum]
        filters = []
        if user:
            filters.append('user={}'.format(user))
        if name:
            filters.append('name={}'.format(name))
        if status:
            if isinstance(status, status_type):
                filters.append('status={}'.format(status))
            else:
                filters.extend(sorted(['status={}'.format(s) for s in status]))
        filters = '&filter=' + ';'.join(filters) if filters else ''
        offset = 1
        chunk = limit if limit else 500
        jobs = []
        while True:
            result = self._get('jobs?jobtype={}{}&offset={}&len={}'.format(job_type, filters, offset, chunk))
            jobs.extend(result[result_type])
            offset += chunk
            if (offset > result['total']) or (limit and offset > limit):
                break

        artifact_type = self.JOB_TYPES[type_enum]
        if details:
            return [artifact_type(self, job).fill_in_details() for job in jobs]
        else:
            return [artifact_type(self, job) for job in jobs]

    def jobs_all_workflows(self, name=None, user=None, limit=0):
        return self._jobs_query(ArtifactType.Workflow, name=name, user=user, limit=limit)

    def jobs_all_active_workflows(self, user=None):
        return self._jobs_query(ArtifactType.Workflow, status=Workflow.Status.active(), user=user)

    def jobs_all_running_workflows(self, user=None):
        return self._jobs_query(ArtifactType.Workflow, status=Workflow.Status.running(), user=user)

    def jobs_running_workflows(self, name, user=None):
        return self._jobs_query(ArtifactType.Workflow, name=name, status=Workflow.Status.running(), user=user)

    def jobs_last_workflow(self, name, user=None):
        jobs = self._jobs_query(ArtifactType.Workflow, name=name, user=user, limit=1)
        if jobs:
            return jobs[-1]
        else:
            raise OozieException.workflow_not_found(name)

    def jobs_workflow_names(self, user=None):
        jobs = self._jobs_query(ArtifactType.Workflow, user=user, details=False)
        return set([job.appName for job in jobs])

    def jobs_all_coordinators(self, name=None, user=None, limit=0):
        return self._jobs_query(ArtifactType.Coordinator, name=name, user=user, limit=limit)

    def jobs_all_active_coordinators(self, user=None):
        return self._jobs_query(ArtifactType.Coordinator, status=Coordinator.Status.active(), user=user)

    def jobs_all_running_coordinators(self, user=None):
        return self._jobs_query(ArtifactType.Coordinator, status=Coordinator.Status.running(), user=user)

    def jobs_running_coordinators(self, name, user=None):
        return self._jobs_query(ArtifactType.Coordinator, name=name, status=Coordinator.Status.running(), user=user)

    def jobs_last_coordinator(self, name, user=None):
        jobs = self._jobs_query(ArtifactType.Coordinator, name=name, user=user, limit=1)
        if jobs:
            return jobs[-1]
        else:
            raise OozieException.coordinator_not_found(name)

    def jobs_coordinator_names(self, user=None):
        coords = self._jobs_query(ArtifactType.Coordinator, user=user, details=False)
        return set([coord.coordJobName for coord in coords])

    """
    ===========================================================================
    Job API - query coordinator details and actions
    ===========================================================================
    """

    def _coordinator_query(self, job_id, status=None, start=0, limit=0):
        id, action = parse_coordinator_id(job_id)
        if not id:
            raise ValueError("Unrecognized job ID: '{}'".format(job_id))
        else:
            if action:
                if start or limit:
                    raise ValueError("Cannot supply both coordinator action ID and start / limit")
                if status:
                    raise ValueError("Cannot supply both coordinator action ID and status")
                start = int(action)
                limit = 1

        filters = []
        if status:
            if isinstance(status, CoordinatorAction.Status):
                filters.append('status={}'.format(status))
            else:
                filters.extend(['status={}'.format(s) for s in status])
        filters = '&filter=' + ';'.join(filters) if filters else ''

        try:
            if start == 0 and limit:
                # Fetch the most recent `limit` actions
                length = limit
                result = self._get('job/{}?order=desc&offset=1&len={}{}'.format(id, length, filters))
            elif limit:
                # Fetch the specified range of actions
                offset = start
                length = limit
                result = self._get('job/{}?offset={}&len={}{}'.format(id, offset, length, filters))
            else:
                # Fetch all actions from `start` onward
                # Ask for 1 first to get the total
                offset = start or 1
                result = self._get('job/{}?offset={}&len=1{}'.format(id, offset, filters))
                total = result['total']
                if total > 0:
                    length = total - offset + 1
                    if length != 1:  # Don't re-ask if we have the answer!
                        result = self._get('job/{}?offset={}&len={}{}'.format(id, offset, length, filters))
        except OozieException as err:
            raise OozieException.coordinator_not_found(job_id, err)

        coord = self.JOB_TYPES[ArtifactType.Coordinator](self, result)
        if action and coord:
            # There's no guarantee that the Nth job is action N
            # Ensure the one requested is loaded
            coord.action(action)
        return coord

    def _coordinator_action_query(self, coordinator_id, action, coordinator=None):
        try:
            result = self._get('job/{}@{}'.format(coordinator_id, action))
        except OozieException as err:
            raise OozieException.coordinator_action_not_found(coordinator_id, action, err)
        coord_action = self.JOB_TYPES[ArtifactType.CoordinatorAction](self, result, parent=coordinator)
        if coordinator:
            coordinator.actions[action] = coord_action
        return coord_action

    def _decode_coord_id(self, coordinator_id=None, name=None, user=None, coordinator=None):
        if coordinator:
            if coordinator_id or name:
                raise ValueError("Supply either a coordinator object or one of coordinator_id or name")
            if user:
                raise ValueError("User parameter not supported with coordinator object")
            result = coordinator.coordJobId
        else:
            if bool(coordinator_id) == bool(name):
                raise ValueError("Supply exactly one of coordinator_id or name")

            result = coordinator_id
            if name:
                coord = self.jobs_last_coordinator(name=name, user=user)
                if coord:
                    result = coord.coordJobId
                else:
                    raise OozieException.coordinator_not_found(name)
            elif user:
                raise ValueError("User parameter not supported with coordinator_id")
        return result

    def job_coordinator_info(self, coordinator_id=None, name=None, user=None, limit=0):
        id = self._decode_coord_id(coordinator_id, name, user)
        return self._coordinator_query(id, limit=limit)

    def job_last_coordinator_info(self, coordinator_id=None, name=None, user=None):
        id = self._decode_coord_id(coordinator_id, name, user)
        return self._coordinator_query(id, limit=1)

    def job_coordinator_action(self, coordinator_id=None, name=None, user=None, action_number=0, coordinator=None):
        id = self._decode_coord_id(coordinator_id, name, user, coordinator)
        if coordinator_id:
            id, action = parse_coordinator_id(coordinator_id)
            if bool(action) == bool(action_number):
                raise ValueError("Supply exactly one of coordinator_id or action_number")
            action_number = action or action_number
        else:
            if action_number == 0:
                raise ValueError("No action_number supplied")

        return self._coordinator_action_query(id, action_number, coordinator=coordinator)

    def job_coordinator_all_active_actions(self, coordinator_id=None, name=None, user=None, coordinator=None):
        id = self._decode_coord_id(coordinator_id, name, user, coordinator)
        coord = self._coordinator_query(id, status=CoordinatorAction.Status.active())
        if coordinator:
            # Copy over any actions to the existing object
            coordinator.actions = coordinator.actions or {}
            for number, action in coord.actions.iteritems():
                coordinator.actions[number] = action
                action._parent = coordinator
            coord = coordinator
        return [action for action in coord.actions.values() if action.status.is_active()]

    """
    ===========================================================================
    Job API - query workflow details and actions
    ===========================================================================
    """

    def _workflow_query(self, job_id):
        id, _ = parse_workflow_id(job_id)
        if not id:
            raise ValueError("Unrecognized job ID: '{}'".format(job_id))
        try:
            result = self._get('job/' + id)
            wf = self.JOB_TYPES[ArtifactType.Workflow](self, result)
            return wf
        except OozieException as err:
            raise OozieException.workflow_not_found(job_id, err)

    def _decode_wf_id(self, workflow_id=None, name=None, user=None):
        if bool(workflow_id) == bool(name):
            raise ValueError("Supply exactly one of workflow_id or name")

        result = workflow_id
        if name:
            wf = self.jobs_last_workflow(name=name, user=user)
            if wf:
                result = wf.id
            else:
                raise OozieException.workflow_not_found(name)
        elif user:
            raise ValueError("User parameter not supported with workflow_id")

        return result

    def job_workflow_info(self, workflow_id=None, name=None, user=None):
        id = self._decode_wf_id(workflow_id, name, user)
        return self._workflow_query(id)

    """
    ===========================================================================
    Job API - query generic job details and actions
    ===========================================================================
    """

    def job_info(self, job_id):
        id, _ = parse_coordinator_id(job_id)
        if id:
            return self.job_coordinator_info(coordinator_id=job_id)

        id, _ = parse_workflow_id(job_id)
        if id:
            return self.job_workflow_info(workflow_id=job_id)

        raise OozieException.job_not_found(job_id)

    def job_action_info(self, job_id):
        id, action = parse_coordinator_id(job_id)
        if id:
            coord = self.job_coordinator_info(coordinator_id=job_id)
            return coord.action(action) if coord and action else coord

        id, action = parse_workflow_id(job_id)
        if id:
            wf = self.job_workflow_info(workflow_id=job_id)
            return wf.action(action) if wf and action else wf

        raise OozieException.job_not_found(job_id)

    """
    ===========================================================================
    Job API - manage coordinator
    ===========================================================================
    """

    def _coordinator_perform_simple_action(self, coord, action):
        if coord.is_action():
            self._put('job/{}?action={}&type=action&scope={}'.format(coord.coordJobId, action, coord.actionNumber))
        else:
            self._put('job/{}?action={}'.format(coord.coordJobId, action))

    def _fetch_coordinator_or_action(self, coordinator_id=None, name=None, user=None):
        id = self._decode_coord_id(coordinator_id, name, user)
        coord = self.job_action_info(id)
        return coord

    def job_coordinator_suspend(self, coordinator_id=None, name=None, user=None):
        coord = self._fetch_coordinator_or_action(coordinator_id, name, user)
        if coord.status.is_suspendable():
            self._coordinator_perform_simple_action(coord, 'suspend')
            return True
        return False

    def job_coordinator_resume(self, coordinator_id=None, name=None, user=None):
        coord = self._fetch_coordinator_or_action(coordinator_id, name, user)
        if coord.status.is_suspended():
            self._coordinator_perform_simple_action(coord, 'resume')
            return True
        return False

    def job_coordinator_kill(self, coordinator_id=None, name=None, user=None):
        coord = self._fetch_coordinator_or_action(coordinator_id, name, user)
        if coord.status.is_active():
            self._coordinator_perform_simple_action(coord, 'kill')
            return True
        return False

    """
    ===========================================================================
    Job API - manage workflow
    ===========================================================================
    """

    def job_workflow_suspend(self, workflow_id=None, name=None, user=None):
        wf = self.job_workflow_info(workflow_id, name, user)
        if wf.status.is_suspendable():
            self._put('job/{}?action=suspend'.format(wf.id))
            return True
        return False

    def job_workflow_resume(self, workflow_id=None, name=None, user=None):
        wf = self.job_workflow_info(workflow_id, name, user)
        if wf.status.is_suspended():
            self._put('job/{}?action=resume'.format(wf.id))
            return True
        return False

    def job_workflow_start(self, workflow_id=None, name=None, user=None):
        wf = self.job_workflow_info(workflow_id, name, user)
        if wf.status == Workflow.Status.PREP:
            self._put('job/{}?action=start'.format(wf.id))
            return True
        return False

    def job_workflow_kill(self, workflow_id=None, name=None, user=None):
        wf = self.job_workflow_info(workflow_id, name, user)
        if wf.status.is_active():
            self._put('job/{}?action=kill'.format(wf.id))
            return True
        return False

    """
    ===========================================================================
    Job API - submit jobs
    ===========================================================================
    """

    def jobs_submit_coordinator(self, hdfs_path, additional_properties=None):
        user = self._user or 'oozie'
        conf = generate_coordinator_submission_xml(user, hdfs_path, additional_properties=additional_properties)
        if self._verbose:
            logger.info('Preparing to submit coordinator {}:\n{}'.format(hdfs_path, conf))
        reply = self._post('jobs', conf)
        if reply and 'id' in reply:
            if self._verbose:
                logger.info('New coordinator: {}'.format(reply['id']))
            coord = self.job_coordinator_info(coordinator_id=reply['id'])
            return coord
        raise OozieException.operation_failed('submit coordinator')

    def jobs_submit_workflow(self, hdfs_path, additional_properties=None, start=False):
        user = self._user or 'oozie'
        conf = generate_workflow_submission_xml(user, hdfs_path, additional_properties=additional_properties)
        if self._verbose:
            logger.info('Preparing to submit workflow {}:\n{}'.format(hdfs_path, conf))
        endpoint = 'jobs?action=start' if start else 'jobs'
        reply = self._post(endpoint, conf)
        if reply and 'id' in reply:
            if self._verbose:
                logger.info('New workflow: {}'.format(reply['id']))
            wf = self.job_workflow_info(workflow_id=reply['id'])
            return wf
        raise OozieException.operation_failed('submit workflow')
