# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import datetime
import os

import pyoozie
import pytest
import pywebhdfs.webhdfs
import six


@pytest.mark.skipif(not bool(os.environ.get(str('INTERACTIVE'))), reason='Requires INTERACTIVE=1 env var')
def test_pyoozie_typical_use_case():

    # Configure
    workflow_path = '/user/oozie/test_workflow.xml'
    coord_path = '/user/oozie/test_coordinator.xml'
    hadoop_user_name = 'root'
    name_node = 'hdfs://hdfs-namenode:9000'
    job_tracker = 'resourcemanager:8032'

    configuration = {
        'oozie.launcher.mapreduce.job.ubertask.enable': 'false',
    }

    # Create XML
    workflow_xml = pyoozie.WorkflowApp(
        name='integration',
        entities=pyoozie.Action(
            action=pyoozie.Shell(
                exec_command="echo",
                arguments="test",
                name_node=name_node,
                job_tracker=job_tracker,
                configuration=configuration,
            )
        )
    ).xml(indent=True)
    print('Created workflow XML')
    print(workflow_xml.decode('utf-8'))

    coord_xml = pyoozie.CoordinatorApp(
        name='integration',
        workflow_app_path=workflow_path,
        frequency=5,
        start=datetime.datetime.now(),
        end=datetime.datetime(2115, 1, 1, 10, 56),
        concurrency=1,
        timeout=5,
        execution_order=pyoozie.ExecutionOrder.LAST_ONLY,
    ).xml(indent=True)
    print('Created coordinator XML')
    print(coord_xml.decode('utf-8'))

    # Store on HDFS
    hdfs_client = pywebhdfs.webhdfs.PyWebHdfsClient(host='localhost', port='14000', user_name=hadoop_user_name)
    for path, data in {workflow_path: workflow_xml, coord_path: coord_xml}.items():
        hdfs_client.create_file(path=path, file_data=data, overwrite=True)
        status = hdfs_client.get_file_dir_status(path)
        assert status and status['FileStatus']['type'] == 'FILE'
        print('Wrote to HDFS %s' % path)

    # Submit coordinator to Oozie
    oozie_client = pyoozie.OozieClient(
        url='http://localhost:11000',
        user=hadoop_user_name,
    )
    coord_config = {
        'user.name': hadoop_user_name,
        'custom.config': '😢',
    }
    print('Submitting coordinator to Oozie')
    coordinator = oozie_client.jobs_submit_coordinator(
        coord_path,
        coord_config,
    )

    # Test that all is well
    print('Coordinator %s created' % coordinator.coordJobId)
    assert coordinator.status.is_active

    # Prompt
    six.moves.input("Press enter to delete coordinator")
    print('Deleting %s' % coordinator.coordJobId)
    assert oozie_client.job_coordinator_kill(coordinator.coordJobId)
