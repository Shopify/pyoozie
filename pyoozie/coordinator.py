# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a BSD-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

from datetime import timedelta
from enum import Enum

from pyoozie.tags import Xml, Parameters, validate


class ExecutionOrder(Enum):
    """Execution order used for coordinator jobs."""

    FIFO = 'FIFO'

    LIFO = 'LIFO'

    LAST_ONLY = 'LAST_ONLY'
    # "When LAST_ONLY is set, an action that is WAITING or READY will be SKIPPED when the current time is past the
    # next action's nominal time.  For example, suppose action 1 and 2 are both WAITING , the current time is
    # 5:00pm, and action 2's nominal time is 5:10pm. In 10 minutes from now, at 5:10pm, action 1 will become
    # SKIPPED, assuming it doesn't transition to SUBMITTED (or a terminal state) before then. Another way of
    # thinking about this is to view it as similar to setting the timeout equal to the frequency, except that the
    # SKIPPED status doesn't cause the coordinator job to eventually become DONEWITHERROR and can actually become
    # SUCCEEDED (i.e. it's a "good" version of TIMEDOUT ). LAST_ONLY is useful if you want a recurring job, but do
    # not actually care about the individual instances and just always want the latest action."

    NONE = 'NONE'
    # "Similar to LAST_ONLY except all older materializations are skipped. When NONE is set, an action that is
    # WAITING or READY will be SKIPPED when the current time is more than a certain configured number of minutes
    # (tolerance) past the action's nominal time."

    def __str__(self):
        return self.value


def format_datetime(value):
    return value.strftime('%Y-%m-%dT%H:%MZ')


class Coordinator(Xml):

    def __init__(self, name, workflow_app_path, frequency, start, end=None, timezone=None,
                 workflow_configuration=None, timeout=None, concurrency=None, execution_order=None, throttle=None,
                 parameters=None):
        super(Coordinator, self).__init__('coordinator-app')
        # Compose and validate dates/frequencies
        if end is None:
            end = start + timedelta(days=100 * 365.24)
        assert end > start, "End time (%s) must be greater than the start time (%s)" % \
            (format_datetime(end), format_datetime(start))
        assert frequency >= 5, "Frequency (%d min) must be greater than or equal to 5 min" % frequency

        # Coordinator
        self.name = validate(name)
        self.frequency = frequency
        self.start = start
        self.end = end
        self.timezone = timezone if timezone else 'UTC'

        # Workflow action
        self.workflow_app_path = workflow_app_path
        self.workflow_configuration = workflow_configuration

        # Controls
        self.timeout = timeout
        self.concurrency = concurrency
        self.execution_order = execution_order
        self.throttle = throttle

        self.parameters = Parameters(parameters)

    def _xml(self, doc, tag, text):
        with tag(self.xml_tag, xmlns="uri:oozie:coordinator:0.5", name=self.name, frequency=str(self.frequency),
                 start=format_datetime(self.start), end=format_datetime(self.end), timezone=self.timezone):

            if self.parameters:
                self.parameters._xml(doc, tag, text)

            if self.timeout or self.concurrency or self.execution_order or self.throttle:
                with tag('controls'):
                    if self.timeout:
                        with tag('timeout'):
                            text(str(self.timeout))
                    if self.concurrency:
                        with tag('concurrency'):
                            text(str(self.concurrency))
                    if self.execution_order:
                        with tag('execution'):
                            text(str(self.execution_order))
                    if self.throttle:
                        with tag('throttle'):
                            text(str(self.throttle))

            with tag('action'):
                with tag('workflow'):
                    with tag('app-path'):
                        text(self.workflow_app_path)
                    if self.workflow_configuration:
                        self.workflow_configuration._xml(doc, tag, text)

        return doc
