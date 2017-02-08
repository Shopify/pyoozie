# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals
from abc import ABCMeta, abstractmethod
import re
import yattag


MAX_IDENTIFIER_LENGTH = 39
REGEX_IDENTIFIER = r'^[a-zA-Z_][\-_a-zA-Z0-9]{0,38}$'
COMPILED_REGEX_IDENTIFIER = re.compile(REGEX_IDENTIFIER)


def _validate(identifier):

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

    __metaclass__ = ABCMeta

    def __init__(self, xml_tag):
        self.xml_tag = xml_tag

    def xml(self, indent=False):
        doc, tag, text = yattag.Doc().tagtext()
        doc.asis("<?xml version='1.0' encoding='UTF-8'?>")
        xml = self._xml(doc, tag, text).getvalue()
        if indent:
            return yattag.indent(xml, indentation=' ' * 4, newline='\r\n')
        else:
            return xml

    @abstractmethod
    def _xml(self, doc, tag, text):
        raise NotImplementedError

    def __str__(self):
        return self.xml_tag


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
        XMLSerializable.__init__(self, xml_tag)
        if values:
            dict.__init__(self, values)
        else:
            dict.__init__(self)
        self.attributes = attributes or {}

    def _xml(self, doc, tag, text):
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
        _PropertyList.__init__(self, 'parameters', values=values)


class Configuration(_PropertyList):
    """Coordinator job submission, workflow, workflow action configuration XML."""

    def __init__(self, values=None):
        _PropertyList.__init__(self, 'configuration', values=values)


class Credentials(_PropertyList):
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
        _PropertyList.__init__(self, 'credentials',
                               attributes={
                                   'name': credential_name,
                                   'type': credential_type,
                               },
                               values=values)
        self.name = _validate(credential_name)


class Shell(XMLSerializable):
    """Workflow shell action (v0.3)."""

    def __init__(self, exec_command, job_tracker=None, name_node=None, prepares=None, job_xml_files=None,
                 configuration=None, arguments=None, env_vars=None, files=None, archives=None, capture_output=False):
        XMLSerializable.__init__(self, 'shell')
        self.exec_command = exec_command
        self.job_tracker = job_tracker
        self.name_node = name_node
        self.prepares = prepares if prepares else list()
        self.job_xml_files = job_xml_files if job_xml_files else list()
        self.configuration = Configuration(configuration)
        self.arguments = arguments if arguments else list()
        self.env_vars = env_vars if env_vars else dict()
        self.files = files if files else list()
        self.archives = archives if archives else list()
        self.capture_output = capture_output

    def _xml(self, doc, tag, text):
        with tag(self.xml_tag, xmlns='uri:oozie:shell-action:0.3'):
            if self.job_tracker:
                with tag('job-tracker'):
                    doc.text(self.job_tracker)

            if self.name_node:
                with tag('name-node'):
                    doc.text(self.name_node)

            if self.prepares:
                raise NotImplementedError("Shell action's prepares has not yet been implemented")

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

    def __init__(self, app_path, propagate_configuration=True, configuration=None):
        XMLSerializable.__init__(self, 'sub-workflow')
        self.app_path = app_path
        self.propagate_configuration = propagate_configuration
        self.configuration = Configuration(configuration)

    def _xml(self, doc, tag, text):
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

    def __init__(self, job_tracker=None, name_node=None, job_xml_files=None, configuration=None):
        XMLSerializable.__init__(self, 'global')
        self.job_tracker = job_tracker
        self.name_node = name_node
        self.job_xml_files = job_xml_files if job_xml_files else list()
        self.configuration = Configuration(configuration)

    def _xml(self, doc, tag, text):
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

    def __init__(self, to, subject, body, cc=None, bcc=None, content_type=None, attachments=None):
        XMLSerializable.__init__(self, 'email')
        self.to = to
        self.subject = subject
        self.body = body
        self.cc = cc
        self.bcc = bcc
        self.content_type = content_type
        self.attachments = attachments

    def _xml(self, doc, tag, text):
        def format_list(emails):
            if hasattr(emails, '__iter__') and not isinstance(emails, str):
                return ','.join(sorted(emails))
            else:
                return emails

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
