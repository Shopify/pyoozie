from __future__ import unicode_literals


import requests


class OozieException(Exception):

    def __init__(self, message='', caused_by=None):
        super(OozieException, self).__init__(message or caused_by and caused_by.message)
        self.caused_by = caused_by

    @classmethod
    def coordinator_not_found(cls, id, caused_by=None):
        return OozieArtifactNotFoundException("Coordinator '{}' not found".format(id), caused_by)

    @classmethod
    def coordinator_action_not_found(cls, id, action, caused_by=None):
        return OozieArtifactNotFoundException("Coordinator action '{}@{}' not found".format(id, action), caused_by)

    @classmethod
    def workflow_not_found(cls, id, caused_by=None):
        return OozieArtifactNotFoundException("Workflow '{}' not found".format(id), caused_by)

    @classmethod
    def workflow_action_not_found(cls, id, action, caused_by=None):
        return OozieArtifactNotFoundException("Workflow action '{}@{}' not found".format(id, action), caused_by)

    @classmethod
    def job_not_found(cls, id, caused_by=None):
        return OozieArtifactNotFoundException("'{}' does not match any known job".format(id), caused_by)

    @classmethod
    def operation_failed(cls, op, caused_by=None):
        return OozieOperationFailedException("Operation failed: {}".format(op), caused_by)

    @classmethod
    def parse_error(cls, message, caused_by=None):
        return OozieParsingException(message, caused_by)

    @classmethod
    def required_key_missing(cls, key, artifact, caused_by=None):
        return OozieParsingException("Required key '{}' missing or invalid in {}".format(key, artifact.__class__.__name__), caused_by)

    @classmethod
    def communication_error(cls, message=None, caused_by=None):
        if not message:
            if caused_by and isinstance(caused_by, requests.RequestException):
                if caused_by.response is not None:
                    message = caused_by.response.headers.get('oozie-error-message', caused_by.response.reason)
        return OozieCommunicationException(message, caused_by)


class OozieArtifactNotFoundException(OozieException):
    pass


class OozieOperationFailedException(OozieException):
    pass


class OozieParsingException(OozieException):
    pass


class OozieCommunicationException(OozieException):
    pass
