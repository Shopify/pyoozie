# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals
import abc
import datetime
import re
import string  # pylint: disable=deprecated-module

import enum
import typing  # pylint: disable=unused-import

import six
import yattag

MAX_NAME_LENGTH = 255
MAX_IDENTIFIER_LENGTH = 50
REGEX_IDENTIFIER = r'^[a-zA-Z_][\-_a-zA-Z0-9]{0,%i}$' % (MAX_IDENTIFIER_LENGTH - 1)
COMPILED_REGEX_IDENTIFIER = re.compile(REGEX_IDENTIFIER)
ALLOWABLE_NAME_CHARS = set(string.ascii_letters + string.punctuation + string.digits + ' ')
ONE_HUNDRED_YEARS = 100 * 365.24


class ExecutionOrder(enum.Enum):
    """Execution order used for coordinator jobs."""

    FIFO = 'FIFO'
    LAST_ONLY = 'LAST_ONLY'
    LIFO = 'LIFO'
    NONE = 'NONE'

    def __str__(self):
        return self.value


EXEC_FIFO = ExecutionOrder.FIFO
EXEC_LAST_ONLY = ExecutionOrder.LAST_ONLY
EXEC_LIFO = ExecutionOrder.LIFO
EXEC_NONE = ExecutionOrder.NONE


def validate_xml_name(name):
    # type: (typing.Text) -> typing.Text

    assert len(name) <= MAX_NAME_LENGTH, \
        "Name must be less than {max_length} chars long, '{name}' is {length}".format(
            max_length=MAX_NAME_LENGTH,
            name=name,
            length=len(name))

    assert all(c in ALLOWABLE_NAME_CHARS for c in name), \
        "Name must be comprised of printable ASCII characters, '{name}' is not".format(name=name)

    return name


def validate_xml_id(identifier):
    # type: (typing.Text) -> typing.Text

    assert len(identifier) <= MAX_IDENTIFIER_LENGTH, \
        "Identifier must be less than {max_length} chars long, '{identifier}' is {length}".format(
            max_length=MAX_IDENTIFIER_LENGTH,
            identifier=identifier,
            length=len(identifier))

    assert COMPILED_REGEX_IDENTIFIER.match(identifier), \
        "Identifier must match {regex}, '{identifier}' does not".format(
            regex=REGEX_IDENTIFIER,
            identifier=identifier)

    return identifier


class XMLSerializable(object):
    """An abstract object that can be serialized to XML."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, xml_tag):
        # type: (typing.Text) -> None
        self.xml_tag = xml_tag

    def xml(self, indent=False):
        # type: (bool) -> str
        doc, tag, text = yattag.Doc().tagtext()
        doc.asis("<?xml version='1.0' encoding='UTF-8'?>")
        xml = self._xml(doc, tag, text).getvalue()
        if indent:
            xml = yattag.indent(xml, indentation=' ' * 4, newline='\r\n')
        return xml.encode('utf-8')

    @abc.abstractmethod
    def _xml(self, doc, tag, text):
        # type: (yattag.doc.Doc, yattag.doc.Doc.tag, yattag.doc.Doc.text) -> yattag.doc.Doc
        raise NotImplementedError()


class _PropertyList(XMLSerializable, dict):
    """
    Object used to represent Oozie workflow/coordinator property-value sets.

    Generates XML of the form:
    ...
    <xml_tag>
      <property>
        <name>[PROPERTY-NAME]</name>
        <value>[PROPERTY-VALUE]</value>
      </property>
      ...
    </xml_tag>
    """

    def __init__(self, xml_tag, attributes=None, values=None):
        # type: (typing.Text, typing.Dict, typing.Dict) -> None
        super(_PropertyList, self).__init__(xml_tag=xml_tag)
        if values:
            self.update(values)
        self.attributes = attributes or {}

    def _xml(self, doc, tag, text):
        # type: (yattag.doc.Doc, yattag.doc.Doc.tag, yattag.doc.Doc.text) -> yattag.doc.Doc
        with tag(self.xml_tag, **self.attributes):
            for name, value in sorted(self.items()):
                with tag('property'):
                    with tag('name'):
                        doc.text('{}'.format(name))
                    with tag('value'):
                        doc.text('{}'.format(value) if value is not None else '')
        return doc


class Parameters(_PropertyList):
    """Coordinator/workflow parameters.

    Allows one to specify properties that can be reused in actions. "Properties that are a valid Java identifier,
    [A-Za-z_][0-9A-Za-z_]* , are available as '${NAME}' variables within the workflow definition."

    "Properties that are not valid Java Identifier, for example 'job.tracker', are available via the
    String wf:conf(String name) function. Valid identifier properties are available via this function as well."
    """

    def __init__(self, values=None):
        # type: (typing.Dict) -> None
        super(Parameters, self).__init__(xml_tag='parameters', values=values)


class Configuration(_PropertyList):
    """Coordinator job submission, workflow, workflow action configuration XML."""

    def __init__(self, values=None):
        # type: (typing.Dict) -> None
        super(Configuration, self).__init__(xml_tag='configuration', values=values)


class Credential(_PropertyList):
    """HCatalog, Hive Metastore, HBase, or Hive Server 2 action credentials.

    Generates XML of the form:
    ```
    ...
    <credentials>
      <credential name='my-hcat-creds' type='hcat'>
         <property>
            <name>hcat.metastore.uri</name>
            <value>HCAT_URI</value>
         </property>
         ...
      </credential>
     </credentials>
     <action name='pig' cred='my-hcat-creds'>
       <pig>
       ...
    ```
    """

    def __init__(self, values, credential_name, credential_type):
        # type: (typing.Dict, typing.Text, typing.Text) -> None
        super(Credential, self).__init__(
            xml_tag='credential',
            attributes={
                'name': credential_name,
                'type': credential_type,
            },
            values=values
        )
        self.name = validate_xml_id(credential_name)


class Shell(XMLSerializable):
    """Workflow shell action (v0.3)."""

    def __init__(
            self,
            exec_command,  # type: typing.Text
            job_tracker=None,  # type: typing.Text
            name_node=None,  # type: typing.Text
            prepare=None,  # type: typing.Sequence
            job_xml_files=None,  # type: typing.Iterable[typing.Text]
            configuration=None,  # type: typing.Dict[typing.Text, typing.Text]
            arguments=None,  # type: typing.Iterable[typing.Text]
            env_vars=None,  # type: typing.Dict[typing.Text, typing.Text]
            files=None,  # type: typing.Iterable[typing.Text]
            archives=None,  # type: typing.Iterable[typing.Text]
            capture_output=False  # type: bool
    ):
        # type: (...) -> None
        super(Shell, self).__init__(xml_tag='shell')
        self.exec_command = exec_command
        self.job_tracker = job_tracker
        self.name_node = name_node
        self.prepare = prepare if prepare else []
        self.job_xml_files = job_xml_files if job_xml_files else []
        self.configuration = Configuration(configuration)
        self.arguments = arguments if arguments else []
        self.env_vars = env_vars if env_vars else {}
        self.files = files if files else []
        self.archives = archives if archives else []
        self.capture_output = capture_output

    def _xml(self, doc, tag, text):
        # type: (yattag.doc.Doc, yattag.doc.Doc.tag, yattag.doc.Doc.text) -> yattag.doc.Doc
        with tag(self.xml_tag, xmlns='uri:oozie:shell-action:0.3'):
            if self.job_tracker:
                with tag('job-tracker'):
                    doc.text(self.job_tracker)

            if self.name_node:
                with tag('name-node'):
                    doc.text(self.name_node)

            if self.prepare:
                raise NotImplementedError("Shell action's prepare has not yet been implemented")

            for xml_file in self.job_xml_files:
                with tag('job-xml'):
                    doc.text(xml_file)

            if self.configuration:
                self.configuration._xml(doc, tag, text)

            with tag('exec'):
                doc.text(self.exec_command)

            for argument in self.arguments:
                with tag('argument'):
                    doc.text(argument)

            for key, value in self.env_vars.items():
                with tag('env-var'):
                    doc.text('{key}={value}'.format(key=key, value=value))

            for filename in self.files:
                with tag('file'):
                    doc.text(filename)

            for archive in self.archives:
                with tag('archive'):
                    doc.text(archive)

            if self.capture_output:
                doc.stag('capture-output')

        return doc


class SubWorkflow(XMLSerializable):
    """Run another workflow defined in another XML file on HDFS.

    An Oozie sub-workflow is an "action [that] runs a child workflow job [...]. The parent workflow job will wait
    until the child workflow job has completed."
    """

    def __init__(
            self,
            app_path,  # type: str
            propagate_configuration=True,  # type: bool
            configuration=None  # type: typing.Dict[typing.Text, typing.Text]
    ):
        # type: (...) -> None
        super(SubWorkflow, self).__init__(xml_tag='sub-workflow')
        self.app_path = app_path
        self.propagate_configuration = propagate_configuration
        self.configuration = Configuration(configuration)

    def _xml(self, doc, tag, text):
        # type: (yattag.doc.Doc, yattag.doc.Doc.tag, yattag.doc.Doc.text) -> yattag.doc.Doc
        with tag(self.xml_tag):
            with tag('app-path'):
                doc.text(self.app_path)
            if self.propagate_configuration:
                doc.stag('propagate-configuration')
            if self.configuration:
                self.configuration._xml(doc, tag, text)

        return doc


class GlobalConfiguration(XMLSerializable):
    """Global configuration values for all actions in a workflow.

    "Oozie allows a global section to reduce the redundant job-tracker and name-node declarations for each action.
    [...] The global section may contain the job-xml, configuration, job-tracker, or name-node that the user would
    like to set for every action.  If a user then redefines one of these in a specific action node, Oozie will
    update [sic] use the specific declaration instead of the global one for that action."

    "The job-xml element, if present, must refer to a Hadoop JobConf job.xml file bundled in the workflow
    application."
    """

    def __init__(
            self,
            job_tracker=None,  # type: typing.Text
            name_node=None,  # typing.Text
            job_xml_files=None,  # type: typing.Iterable[typing.Text]
            configuration=None,  # type: typing.Dict[typing.Text, typing.Text]
    ):
        # type: (...) -> None
        super(GlobalConfiguration, self).__init__(xml_tag='global')
        self.job_tracker = job_tracker
        self.name_node = name_node
        self.job_xml_files = job_xml_files if job_xml_files else []
        self.configuration = Configuration(configuration)

    def _xml(self, doc, tag, text):
        # type: (yattag.doc.Doc, yattag.doc.Doc.tag, yattag.doc.Doc.text) -> yattag.doc.Doc
        with tag(self.xml_tag):
            if self.job_tracker:
                with tag('job-tracker'):
                    doc.text(self.job_tracker)
            if self.name_node:
                with tag('name-node'):
                    doc.text(self.name_node)
            if self.job_xml_files:
                for xml_file in self.job_xml_files:
                    with tag('job-xml'):
                        doc.text(xml_file)
            if self.configuration:
                self.configuration._xml(doc, tag, text)

        return doc


class Email(XMLSerializable):
    """Email action for use within a workflow."""

    def __init__(
            self,
            to,  # type: typing.Union[typing.Text, typing.Iterable[typing.Text]]
            subject,  # type: typing.Text
            body,  # type: typing.Text
            cc=None,  # type: typing.Union[typing.Text, typing.Iterable[typing.Text]]
            bcc=None,  # type: typing.Union[typing.Text, typing.Iterable[typing.Text]]
            content_type=None,  # type: typing.Text
            attachments=None  # type: typing.Text
    ):
        # type: (...) -> None
        super(Email, self).__init__(xml_tag='email')
        self.to = to
        self.subject = subject
        self.body = body
        self.cc = cc
        self.bcc = bcc
        self.content_type = content_type
        self.attachments = attachments

    def _xml(self, doc, tag, text):
        # type: (yattag.doc.Doc, yattag.doc.Doc.tag, yattag.doc.Doc.text) -> yattag.doc.Doc

        def format_list(strings):
            if hasattr(strings, '__iter__') and not isinstance(strings, six.string_types):
                return ','.join(sorted(strings))
            else:
                return strings

        with tag(self.xml_tag, xmlns='uri:oozie:email-action:0.2'):
            with tag('to'):
                doc.text(format_list(self.to))
            with tag('subject'):
                doc.text(self.subject)
            with tag('body'):
                doc.text(self.body)
            if self.cc:
                with tag('cc'):
                    doc.text(format_list(self.cc))
            if self.bcc:
                with tag('bcc'):
                    doc.text(format_list(self.bcc))
            if self.content_type:
                with tag('content_type'):
                    doc.text(self.content_type)
            if self.attachments:
                with tag('attachment'):
                    doc.text(format_list(self.attachments))

        return doc


class CoordinatorApp(XMLSerializable):

    def __init__(
            self,
            name,  # type: typing.Text
            workflow_app_path,  # type: typing.Text
            frequency,  # type: int
            start,  # type: datetime.datetime
            end=None,  # type: datetime.datetime
            timezone=None,  # type: typing.Text
            workflow_configuration=None,  # type: typing.Dict[typing.Text, typing.Text]
            timeout=None,  # type: int
            concurrency=None,  # type: int
            execution_order=None,  # type: ExecutionOrder
            throttle=None,  # type: int
            parameters=None  # type: typing.Dict[typing.Text, typing.Text]
    ):
        # type: (...) -> None
        super(CoordinatorApp, self).__init__(xml_tag='coordinator-app')

        # Compose and validate dates/frequencies
        if end is None:
            end = start + datetime.timedelta(days=ONE_HUNDRED_YEARS)
        assert end > start, "End time ({end}) must be greater than the start time ({start})".format(
            end=CoordinatorApp.__format_datetime(end), start=CoordinatorApp.__format_datetime(start))
        assert frequency >= 5, "Frequency ({frequency} min) must be greater than or equal to 5 min".format(
            frequency=frequency)

        # Coordinator
        self.name = validate_xml_name(name)
        self.frequency = frequency
        self.start = start
        self.end = end
        self.timezone = timezone if timezone else 'UTC'

        # Workflow action
        self.workflow_app_path = workflow_app_path
        self.workflow_configuration = Configuration(workflow_configuration)

        # Controls
        self.timeout = timeout
        self.concurrency = concurrency
        self.execution_order = execution_order
        self.throttle = throttle

        self.parameters = Parameters(parameters)

    @staticmethod
    def __format_datetime(value):  # type: (datetime.datetime) -> typing.Text
        return value.strftime('%Y-%m-%dT%H:%MZ')

    def _xml(self, doc, tag, text):
        # type: (yattag.doc.Doc, yattag.doc.Doc.tag, yattag.doc.Doc.text) -> yattag.doc.Doc

        with tag(self.xml_tag, xmlns="uri:oozie:coordinator:0.4", name=self.name, frequency=str(self.frequency),
                 start=CoordinatorApp.__format_datetime(self.start), end=CoordinatorApp.__format_datetime(self.end),
                 timezone=self.timezone):

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
