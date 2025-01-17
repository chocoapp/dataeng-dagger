import logging
import os
import signal
import time

import boto3
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.utils.decorators import apply_defaults

from dagger.dag_creator.airflow.operators.dagger_base_operator import DaggerBaseOperator

ENV = os.environ["ENV"].lower()
ENV_SUFFIX = "dev/" if ENV == "local" else ""


class SparkSubmitOperator(DaggerBaseOperator):
    ui_color = "bisque"
    template_fields = ("job_args", "spark_args", "spark_conf_args")

    @apply_defaults
    def __init__(
            self,
            job_file,
            cluster_name,
            job_args=None,
            spark_args=None,
            spark_conf_args=None,
            spark_app_name=None,
            extra_py_files=None,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.job_file = job_file
        self.job_args = job_args
        self.spark_args = spark_args
        self.spark_conf_args = spark_conf_args
        self.spark_app_name = spark_app_name
        self.extra_py_files = extra_py_files
        self.cluster_name = cluster_name
        self._execution_timeout = kwargs.get('execution_timeout')

    @property
    def emr_client(self):
        return boto3.client("emr")

    @property
    def ssm_client(self):
        return boto3.client("ssm")

    @property
    def spark_submit_cmd(self):
        spark_submit_cmd = "spark-submit --master yarn --deploy-mode cluster"
        if self.spark_args is not None:
            spark_submit_cmd += " " + self.spark_args
        if self.spark_conf_args is not None:
            spark_submit_cmd += " " + self.spark_conf_args
        if self.extra_py_files is not None:
            spark_submit_cmd += " " + f"--py-files {self.extra_py_files}"

        spark_submit_cmd += " " + self.job_file

        logging.info(f"Running spark command: {spark_submit_cmd}")

        if self.job_args is not None:
            spark_submit_cmd += " " + self.job_args
        return spark_submit_cmd

    def get_execution_timeout(self):
        if self._execution_timeout:
            return f"{self._execution_timeout.seconds}"

        return None

    def get_cluster_id_by_name(self, emr_cluster_name, cluster_states):
        response = self.emr_client.list_clusters(ClusterStates=cluster_states)
        matching_clusters = list(
            filter(lambda cluster: cluster['Name'] == emr_cluster_name, response['Clusters']))

        if len(matching_clusters) == 1:
            cluster_id = matching_clusters[0]['Id']
            logging.info('Found cluster name = %s id = %s' % (emr_cluster_name, cluster_id))
            return cluster_id
        elif len(matching_clusters) > 1:
            raise AirflowException('More than one cluster found for name = %s' % emr_cluster_name)
        else:
            return None


    def get_application_id_by_name(self, emr_master_instance_id, application_name):
        """
        Get the application ID of the Spark job
        """
        command = f"yarn application -list -appStates RUNNING | grep {application_name}"

        response = self.ssm_client.send_command(
            InstanceIds=[emr_master_instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [command]}
        )

        command_id = response['Command']['CommandId']
        time.sleep(10)  # Wait for the command to execute

        output = self.ssm_client.get_command_invocation(
            CommandId=command_id,
            InstanceId=emr_master_instance_id
        )

        stdout = output['StandardOutputContent']
        for line in stdout.split('\n'):
            if application_name in line:
                application_id = line.split()[0]
                return application_id
        return None

    def kill_spark_job(self, emr_master_instance_id, application_id):
        """
        Kill the Spark job using YARN
        """
        kill_command = f"yarn application -kill {application_id}"
        self.ssm_client.send_command(
            InstanceIds=[emr_master_instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [kill_command]}
        )
        raise AirflowException(
            f"Spark job exceeded the execution timeout of {self._execution_timeout} seconds and was terminated.")

    def execute(self, context):
        """
        See `execute` method from airflow.operators.bash_operator
        """
        start_time = time.time()
        try:
            # Get cluster and master node information
            cluster_id = self.get_cluster_id_by_name(self.cluster_name, ["WAITING", "RUNNING"])
            emr_master_instance_id = self.emr_client.list_instances(
                ClusterId=cluster_id, InstanceGroupTypes=["MASTER"], InstanceStates=["RUNNING"]
            )["Instances"][0]["Ec2InstanceId"]

            # Build the command parameters
            command_parameters = {"commands": [self.spark_submit_cmd]}
            if self._execution_timeout:
                command_parameters["executionTimeout"] = [self.get_execution_timeout()]

            # Send the command via SSM
            response = self.ssm_client.send_command(
                InstanceIds=[emr_master_instance_id],
                DocumentName="AWS-RunShellScript",
                Parameters=command_parameters
            )
            command_id = response['Command']['CommandId']
            status = 'Pending'
            status_details = None

            # Monitor the command's execution
            while status in ['Pending', 'InProgress', 'Delayed']:
                time.sleep(30)
                # Check the status of the SSM command
                response = self.ssm_client.get_command_invocation(
                    CommandId=command_id, InstanceId=emr_master_instance_id
                )
                status = response['Status']
                status_details = response['StatusDetails']

            self.log.info(
                self.ssm_client.get_command_invocation(
                    CommandId=command_id, InstanceId=emr_master_instance_id
                )['StandardErrorContent']
            )

            # Raise an exception if the command did not succeed
            if status != 'Success':
                raise AirflowException(f"Spark command failed, check Spark job status in YARN resource manager. "
                                       f"Response status details: {status_details}")

        except AirflowTaskTimeout:
            # Handle task timeout
            self.log.error("Task timed out. Attempting to terminate the Spark job.")
            application_id = self.get_application_id_by_name(
                emr_master_instance_id, self.spark_app_name
            )
            if application_id:
                self.kill_spark_job(emr_master_instance_id, application_id)
            raise AirflowException("Task timed out and the Spark job was terminated.")
