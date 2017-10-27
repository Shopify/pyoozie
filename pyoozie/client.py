# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import logging
import requests

from pyoozie import xml
from pyoozie import exceptions
from pyoozie import model


class OozieClient(object):

    JOB_TYPE_STRINGS = {
        model.ArtifactType.Coordinator: ('coordinator', 'coordinatorjobs'),
        model.ArtifactType.Workflow: ('wf', 'workflows'),
    }

    JOB_TYPES = {
        model.ArtifactType.Coordinator: model.Coordinator,
        model.ArtifactType.CoordinatorAction: model.CoordinatorAction,
        model.ArtifactType.Workflow: model.Workflow,
        model.ArtifactType.WorkflowAction: model.WorkflowAction,
    }

    STATUS_TYPES = {
        model.ArtifactType.Coordinator: model.CoordinatorStatus,
        model.ArtifactType.CoordinatorAction: model.CoordinatorActionStatus,
        model.ArtifactType.Workflow: model.WorkflowStatus,
        model.ArtifactType.WorkflowAction: model.WorkflowActionStatus,
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

        @property
        def requests(self):
            return self._requests

        @property
        def errors(self):
            return self._errors

        @property
        def bytes_received(self):
            return self._bytes_received

        @property
        def elapsed(self):
            return self._elapsed

    def __init__(self, url=None, user=None, timeout=None, verbose=True, **_):
        self.logger = logging.getLogger('pyoozie.OozieClient')
        oozie_url = (url or 'http://localhost').rstrip('/')
        if not oozie_url.endswith('/oozie'):
            oozie_url += '/oozie'
        self._url = oozie_url
        self._user = user
        self._timeout = timeout or 30
        self._verbose = verbose  # Note: change default for verbose!
        self._stats = OozieClient.Stats()
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
                self.logger.error(response.headers)
            message = "Unable to contact Oozie server at {}".format(self._url)
            raise exceptions.OozieException.communication_error(message, err)
        try:
            versions = response.json()
        except ValueError as err:
            message = "Invalid response from Oozie server at {} ".format(self._url)
            raise exceptions.OozieException.communication_error(message, err)
        if 2 not in versions:
            message = "Oozie server at {} does not support API version 2 (supported: {})".format(self._url, versions)
            raise exceptions.OozieException.communication_error(message)

    def _headers(self, content_type=None):
        headers = {}
        if content_type:
            headers['Content-Type'] = content_type
        return headers

    def _request(self, method, endpoint, content_type, content=None):
        response = None
        url = '{}/v2/{}'.format(self._url, endpoint)

        if self._verbose:
            if content:
                self.logger.info("Request: %s %s content bytes: %s", method, url, len(content))
            else:
                self.logger.info("Request: %s %s", method, url)

        try:
            response = requests.request(method, url, data=content, timeout=self._timeout,
                                        headers=self._headers(content_type))
            response.raise_for_status()
        except requests.RequestException as err:
            self._stats.update(response)
            if self._verbose and response is not None:
                self.logger.error("Reply: status=%s reason=%s elapsed=%sms",
                                  response.status_code,
                                  response.reason,
                                  response.elapsed.microseconds / 1000.0)
            raise exceptions.OozieException.communication_error(caused_by=err)

        self._stats.update(response)
        if self._verbose:
            self.logger.info("Reply: status=%s bytes=%s elapsed=%sms",
                             response.status_code,
                             len(response.text),
                             response.elapsed.microseconds / 1000.0)

        try:
            return response.json() if len(response.content) else None
        except ValueError as err:
            message = "Invalid response from Oozie server at {} ".format(self._url)
            raise exceptions.OozieException.communication_error(message, caused_by=err)

    def _get(self, endpoint, content_type=None):
        return self._request('GET', endpoint, content_type)

    def _put(self, endpoint, content=None, content_type='application/xml'):
        return self._request('PUT', endpoint, content_type, content)

    def _post(self, endpoint, content, content_type='application/xml'):
        return self._request('POST', endpoint, content_type, content)

    def report_stats(self, to_logger=None):
        if not to_logger:
            to_logger = self.logger
        to_logger.info(
            "OozieClient Stats: requests=%s errors=%s bytes=%s elapsed=%sms",
            self._stats.requests,
            self._stats.errors,
            self._stats.bytes_received,
            self._stats.elapsed / 1000)

    def reset_stats(self):
        self._stats.reset()

    # ===========================================================================
    # Admin API
    # ===========================================================================

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

    # ===========================================================================
    # Jobs API - query coordinators and workflows
    # ===========================================================================

    def _filter_string(self, type_enum, user=None, name=None, status=None):
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
        return filters

    def _jobs_query(self, type_enum, user=None, name=None, status=None, limit=0, details=True):
        job_type, result_type = self.JOB_TYPE_STRINGS[type_enum]
        filters = self._filter_string(type_enum, user=user, name=name, status=status)
        offset = 1
        chunk = limit if limit else 500
        jobs = []
        while True:
            result = self._get('jobs?jobtype={}{}&offset={}&len={}'.format(job_type, filters, offset, chunk))
            jobs.extend(result[result_type])
            offset += chunk
            if (offset > result['total']) or (limit and offset > limit):
                break

        if details:
            return [self.JOB_TYPES[type_enum](self, job).fill_in_details() for job in jobs]
        else:
            return [self.JOB_TYPES[type_enum](self, job) for job in jobs]

    def jobs_all_workflows(self, name=None, user=None, limit=0):
        return self._jobs_query(model.ArtifactType.Workflow, name=name, user=user, limit=limit)

    def jobs_all_active_workflows(self, user=None):
        return self._jobs_query(model.ArtifactType.Workflow, status=model.WorkflowStatus.active(), user=user)

    def jobs_all_running_workflows(self, user=None):
        return self._jobs_query(model.ArtifactType.Workflow, status=model.WorkflowStatus.running(), user=user)

    def jobs_running_workflows(self, name, user=None):
        return self._jobs_query(
            model.ArtifactType.Workflow, name=name, status=model.WorkflowStatus.running(), user=user)

    def jobs_last_workflow(self, name, user=None):
        jobs = self._jobs_query(model.ArtifactType.Workflow, name=name, user=user, limit=1)
        if jobs:
            return jobs[-1]
        else:
            raise exceptions.OozieException.workflow_not_found(name)

    def jobs_workflow_names(self, user=None):
        jobs = self._jobs_query(model.ArtifactType.Workflow, user=user, details=False)
        return set([job.appName for job in jobs])

    def jobs_all_coordinators(self, name=None, user=None, limit=0):
        return self._jobs_query(model.ArtifactType.Coordinator, name=name, user=user, limit=limit)

    def jobs_all_active_coordinators(self, user=None):
        return self._jobs_query(model.ArtifactType.Coordinator, status=model.CoordinatorStatus.active(), user=user)

    def jobs_all_running_coordinators(self, user=None):
        return self._jobs_query(model.ArtifactType.Coordinator, status=model.CoordinatorStatus.running(), user=user)

    def jobs_all_suspended_coordinators(self, user=None):
        return self._jobs_query(model.ArtifactType.Coordinator, status=model.CoordinatorStatus.suspended(), user=user)

    def jobs_running_coordinators(self, name, user=None):
        return self._jobs_query(
            model.ArtifactType.Coordinator, name=name, status=model.CoordinatorStatus.running(), user=user)

    def jobs_last_coordinator(self, name, user=None):
        jobs = self._jobs_query(model.ArtifactType.Coordinator, name=name, user=user, limit=1)
        if jobs:
            return jobs[-1]
        else:
            raise exceptions.OozieException.coordinator_not_found(name)

    def jobs_coordinator_names(self, user=None):
        coords = self._jobs_query(model.ArtifactType.Coordinator, user=user, details=False)
        return set([coord.coordJobName for coord in coords])

    # ===========================================================================
    # Job API - query coordinator details and actions
    # ===========================================================================

    def _coordinator_query(self, job_id, status=None, start=0, limit=0):
        coord_id, action = model.parse_coordinator_id(job_id)
        if not coord_id:
            raise ValueError("Unrecognized job ID: '{}'".format(job_id))
        else:
            if action:
                if start or limit:
                    raise ValueError("Cannot supply both coordinator action ID and start / limit")
                if status:
                    raise ValueError("Cannot supply both coordinator action ID and status")
                start = int(action)
                limit = 1

        def wrapped_get(uri):
            try:
                return self._get(uri)
            except exceptions.OozieException as err:
                raise exceptions.OozieException.coordinator_not_found(job_id, err)

        filters = self._filter_string(model.ArtifactType.CoordinatorAction, status=status)
        if start == 0 and limit:
            # Fetch the most recent `limit` actions
            length = limit
            result = wrapped_get('job/{}?order=desc&offset=1&len={}{}'.format(coord_id, length, filters))
        elif limit:
            # Fetch the specified range of actions
            offset = start
            length = limit
            result = wrapped_get('job/{}?offset={}&len={}{}'.format(coord_id, offset, length, filters))
        else:
            # Fetch all actions from `start` onward
            # Ask for 1 first to get the total
            offset = start or 1
            result = wrapped_get('job/{}?offset={}&len=1{}'.format(coord_id, offset, filters))
            total = result['total']
            if total > 0:
                length = total - offset + 1
                if length != 1:  # Don't re-ask if we have the answer!
                    result = wrapped_get('job/{}?offset={}&len={}{}'.format(coord_id, offset, length, filters))

        coord = self.JOB_TYPES[model.ArtifactType.Coordinator](self, result)
        if action and coord:
            # There's no guarantee that the Nth job is action N
            # Ensure the one requested is loaded
            coord.action(action)
        return coord

    def _coordinator_action_query(self, coordinator_id, action, coordinator=None):
        try:
            result = self._get('job/{}@{}'.format(coordinator_id, action))
        except exceptions.OozieException as err:
            raise exceptions.OozieException.coordinator_action_not_found(coordinator_id, action, err)
        coord_action = self.JOB_TYPES[model.ArtifactType.CoordinatorAction](self, result, parent=coordinator)
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
                    raise exceptions.OozieException.coordinator_not_found(name)
            elif user:
                raise ValueError("User parameter not supported with coordinator_id")
        return result

    def job_coordinator_info(self, coordinator_id=None, name=None, user=None, limit=0):
        coord_id = self._decode_coord_id(coordinator_id, name, user)
        return self._coordinator_query(coord_id, limit=limit)

    def job_last_coordinator_info(self, coordinator_id=None, name=None, user=None):
        coord_id = self._decode_coord_id(coordinator_id, name, user)
        return self._coordinator_query(coord_id, limit=1)

    def job_coordinator_action(self, coordinator_id=None, name=None, user=None, action_number=0, coordinator=None):
        coord_id = self._decode_coord_id(coordinator_id, name, user, coordinator)
        if coordinator_id:
            coord_id, action = model.parse_coordinator_id(coordinator_id)
            if bool(action) == bool(action_number):
                raise ValueError("Supply exactly one of coordinator_id or action_number")
            action_number = action or action_number
        else:
            if action_number == 0:
                raise ValueError("No action_number supplied")

        return self._coordinator_action_query(coord_id, action_number, coordinator=coordinator)

    def job_coordinator_all_active_actions(self, coordinator_id=None, name=None, user=None, coordinator=None):
        coord_id = self._decode_coord_id(coordinator_id, name, user, coordinator)
        coord = self._coordinator_query(coord_id, status=model.CoordinatorActionStatus.active())
        if coordinator:
            # Copy over any actions to the existing object
            coordinator.actions = coordinator.actions or {}
            for number, action in coord.actions.items():
                action._parent = coordinator
                coordinator.actions[number] = action
            coord = coordinator
        return [action for action in coord.actions.values() if action.status.is_active()]

    # ===========================================================================
    # Job API - query workflow details and actions
    # ===========================================================================

    def _workflow_query(self, job_id):
        wf_id, _ = model.parse_workflow_id(job_id)
        if not wf_id:
            raise ValueError("Unrecognized job ID: '{}'".format(job_id))
        try:
            result = self._get('job/' + wf_id)
            workflow = self.JOB_TYPES[model.ArtifactType.Workflow](self, result)
            return workflow
        except exceptions.OozieException as err:
            raise exceptions.OozieException.workflow_not_found(job_id, err)

    def _decode_wf_id(self, workflow_id=None, name=None, user=None):
        if bool(workflow_id) == bool(name):
            raise ValueError("Supply exactly one of workflow_id or name")

        result = workflow_id
        if name:
            workflow = self.jobs_last_workflow(name=name, user=user)
            if workflow:
                result = workflow.id
            else:
                raise exceptions.OozieException.workflow_not_found(name)
        elif user:
            raise ValueError("User parameter not supported with workflow_id")

        return result

    def job_workflow_info(self, workflow_id=None, name=None, user=None):
        wf_id = self._decode_wf_id(workflow_id, name, user)
        return self._workflow_query(wf_id)

    # ===========================================================================
    # Job API - query generic job details and actions
    # ===========================================================================

    def job_info(self, job_id):
        coord_id, _ = model.parse_coordinator_id(job_id)
        if coord_id:
            return self.job_coordinator_info(coordinator_id=job_id)

        wf_id, _ = model.parse_workflow_id(job_id)
        if wf_id:
            return self.job_workflow_info(workflow_id=job_id)

        raise exceptions.OozieException.job_not_found(job_id)

    def job_action_info(self, job_id):
        coord_id, action = model.parse_coordinator_id(job_id)
        if coord_id:
            coord = self.job_coordinator_info(coordinator_id=job_id)
            return coord.action(action) if coord and action else coord

        wf_id, action = model.parse_workflow_id(job_id)
        if wf_id:
            workflow = self.job_workflow_info(workflow_id=job_id)
            return workflow.action(action) if workflow and action else workflow

        raise exceptions.OozieException.job_not_found(job_id)

    # ===========================================================================
    # Job API - manage coordinator
    # ===========================================================================

    def _coordinator_perform_simple_action(self, coord, action, refresh=True):
        if coord.is_action():
            self._put('job/{}?action={}&type=action&scope={}&refresh={}'.format(coord.coordJobId, action, coord.actionNumber, "true" if refresh else "false"))
        else:
            self._put('job/{}?action={}'.format(coord.coordJobId, action))

    def _fetch_coordinator_or_action(self, coordinator_id=None, name=None, user=None):
        coord_id = self._decode_coord_id(coordinator_id, name, user)
        coord = self.job_action_info(coord_id)
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

    def job_coordinator_rerun(self, coordinator_id=None, name=None, user=None):
        coord = self._fetch_coordinator_or_action(coordinator_id, name, user)
        if not coord.status.is_active():
            self._coordinator_perform_simple_action(coord, 'coord-rerun')
            return True
        return False

    # ===========================================================================
    # Job API - manage workflow
    # ===========================================================================

    def job_workflow_suspend(self, workflow_id=None, name=None, user=None):
        workflow = self.job_workflow_info(workflow_id, name, user)
        if workflow.status.is_suspendable():
            self._put('job/{}?action=suspend'.format(workflow.id))
            return True
        return False

    def job_workflow_resume(self, workflow_id=None, name=None, user=None):
        workflow = self.job_workflow_info(workflow_id, name, user)
        if workflow.status.is_suspended():
            self._put('job/{}?action=resume'.format(workflow.id))
            return True
        return False

    def job_workflow_start(self, workflow_id=None, name=None, user=None):
        workflow = self.job_workflow_info(workflow_id, name, user)
        if workflow.status == model.WorkflowStatus.PREP:
            self._put('job/{}?action=start'.format(workflow.id))
            return True
        return False

    def job_workflow_kill(self, workflow_id=None, name=None, user=None):
        workflow = self.job_workflow_info(workflow_id, name, user)
        if workflow.status.is_active():
            self._put('job/{}?action=kill'.format(workflow.id))
            return True
        return False

    # ===========================================================================
    # Job API - submit and update jobs
    # ===========================================================================

    def jobs_submit_coordinator(self, xml_path, configuration=None):
        user = self._user or 'oozie'
        conf = xml._coordinator_submission_xml(user, xml_path, configuration=configuration)
        if self._verbose:
            self.logger.info('Preparing to submit coordinator %s:\n%s', xml_path, conf)
        reply = self._post('jobs', conf)
        if reply and 'id' in reply:
            if self._verbose:
                self.logger.info('New coordinator: %s', reply['id'])
            coord = self.job_coordinator_info(coordinator_id=reply['id'])
            return coord
        raise exceptions.OozieException.operation_failed('submit coordinator')

    def jobs_update_coordinator(self, coordinator_id, xml_path, configuration=None):
        user = self._user or 'oozie'
        coord = self._fetch_coordinator_or_action(coordinator_id)
        if coord.status.is_active():
            conf = xml._coordinator_submission_xml(user, xml_path, configuration=configuration)
            if self._verbose:
                self.logger.info('Preparing to update coordinator %s:\n%s', xml_path, conf)
            reply = self._put('job/{}?action=update'.format(coordinator_id), conf)

            if not reply or 'update' not in reply:
                raise exceptions.OozieException.operation_failed('update coordinator')

            if self._verbose:
                self.logger.info('Coordinator %s updated with diff %s', coordinator_id, reply['update']['diff'])

            return self.job_coordinator_info(coordinator_id=coordinator_id)
        else:
            raise exceptions.OozieException.operation_failed('coordinator status must be active in order to update')

    def jobs_submit_workflow(self, xml_path, configuration=None, start=False):
        user = self._user or 'oozie'
        conf = xml._workflow_submission_xml(user, xml_path, configuration=configuration)
        if self._verbose:
            self.logger.info('Preparing to submit workflow %s:\n%s', xml_path, conf)
        endpoint = 'jobs?action=start' if start else 'jobs'
        reply = self._post(endpoint, conf)
        if reply and 'id' in reply:
            if self._verbose:
                self.logger.info('New workflow: %s', reply['id'])
            workflow = self.job_workflow_info(workflow_id=reply['id'])
            return workflow
        raise exceptions.OozieException.operation_failed('submit workflow')
