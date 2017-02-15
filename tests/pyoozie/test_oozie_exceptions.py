# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import requests

from pyoozie import exceptions


class TestOozieException(object):

    def test_coordinator_not_found(self):
        result = exceptions.OozieException.coordinator_not_found('bad-coord')
        assert isinstance(result, exceptions.OozieArtifactNotFoundException)
        assert "Coordinator 'bad-coord' not found" in str(result)

    def test_coordinator_action_not_found(self):
        result = exceptions.OozieException.coordinator_action_not_found('bad-coord', 666)
        assert isinstance(result, exceptions.OozieArtifactNotFoundException)
        assert "Coordinator action 'bad-coord@666' not found" in str(result)

    def test_workflow_not_found(self):
        result = exceptions.OozieException.workflow_not_found('bad-wf')
        assert isinstance(result, exceptions.OozieArtifactNotFoundException)
        assert "Workflow 'bad-wf' not found" in str(result)

    def test_workflow_action_not_found(self):
        result = exceptions.OozieException.workflow_action_not_found('bad-wf', 'action')
        assert isinstance(result, exceptions.OozieArtifactNotFoundException)
        assert "Workflow action 'bad-wf@action' not found" in str(result)

    def test_operation_failed(self):
        result = exceptions.OozieException.operation_failed('bad-op')
        assert isinstance(result, exceptions.OozieOperationFailedException)
        assert "Operation failed: bad-op" in str(result)

    def test_parse_error(self):
        result = exceptions.OozieException.parse_error('Syntax error')
        assert isinstance(result, exceptions.OozieParsingException)
        assert 'Syntax error' in str(result)

    def test_required_key_missing(self):
        result = exceptions.OozieException.required_key_missing('key', None)
        assert isinstance(result, exceptions.OozieParsingException)
        assert "Required key 'key' missing or invalid in " in str(result)

    def test_communication_error(self):
        result = exceptions.OozieException.communication_error('Bad request')
        assert isinstance(result, exceptions.OozieCommunicationException)
        assert 'Bad request' in str(result)

    def test_exception_chaining(self):
        inner = exceptions.OozieException.parse_error('Syntax error')
        outer = exceptions.OozieException.coordinator_not_found('bad-coord', inner)
        assert outer.caused_by is inner

        inner2 = exceptions.OozieException.operation_failed('Op failed')
        outer2 = exceptions.OozieException.communication_error(caused_by=inner2)
        assert outer2.caused_by is inner2
        assert 'Op failed' in str(outer2)

    def test_oozie_error_message(self):
        response = requests.Response()
        response.reason = 'Bad request'
        response.headers = {
            'Content-Length': '968',
            'Content-Type': 'text/html;charset=utf-8',
            'oozie-error-code': 'E0605',
            'oozie-error-message': 'E0605: Action does not exist [0123456-123456789012345-oozie-oozi-C@12]',
        }
        inner = requests.HTTPError(response=response)
        outer = exceptions.OozieException.communication_error(caused_by=inner)
        assert 'Action does not exist [0123456-123456789012345-oozie-oozi-C@12]' in str(outer)

        response.headers = {
            'Content-Length': '968',
            'Content-Type': 'text/html;charset=utf-8',
        }
        outer2 = exceptions.OozieException.communication_error(caused_by=inner)
        assert 'Bad request' in str(outer2)
