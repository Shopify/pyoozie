from __future__ import unicode_literals
from starscream.scheduling.oozie.xml import Configuration, Coordinator, \
    Parameters, ExecutionOrder


def generate_workflow_submission_xml(hadoop_username, hdfs_path, additional_properties=None, indent=False):
    """Generate a Workflow XML submission message to POST to Oozie."""
    configuration = Configuration({
        'user.name': hadoop_username,
        'oozie.wf.application.path': hdfs_path
    })
    if additional_properties:
        configuration.update(additional_properties)
    return configuration.xml(indent)


def generate_coordinator_submission_xml(hadoop_username, hdfs_path, additional_properties=None, indent=False):
    """Generate a Coordinator XML submission message to POST to Oozie."""
    configuration = Configuration({
        'user.name': hadoop_username,
        'oozie.coord.application.path': hdfs_path,
    })
    if additional_properties:
        configuration.update(additional_properties)
    return configuration.xml(indent)


def generate_coordinator_xml(name, workflow_app_path, frequency_in_minutes, start, end=None, timezone=None,
                             workflow_configuration=None, timeout_in_minutes=None, concurrency=None,
                             execution_order=None, throttle=None, parameters=None, indent=False):
    """Generate a Coordinator XML definition to store on HDFS."""
    if execution_order and not isinstance(execution_order, ExecutionOrder):
        raise ValueError('Expected enum of type ExecutionOrder')
    return Coordinator(
        name=name,
        workflow_app_path=workflow_app_path,
        frequency=frequency_in_minutes,
        start=start,
        end=end,
        timezone=timezone,
        workflow_configuration=Configuration(workflow_configuration)
        if workflow_configuration else None,
        timeout=timeout_in_minutes,
        concurrency=concurrency,
        execution_order=execution_order,
        throttle=throttle,
        parameters=Parameters(parameters) if parameters else None
    ).xml(indent)
