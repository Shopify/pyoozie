# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

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
