from typing import Any, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.links.batch import (
    BatchJobDefinitionLink,
    BatchJobQueueLink,
)
from airflow.providers.amazon.aws.links.logs import CloudWatchEventsLink
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.utils.context import Context


class AWSBatchOperator(BatchOperator):
    @staticmethod
    def _format_cloudwatch_link(awslogs_region: str, awslogs_group: str, awslogs_stream_name: str):
        return f"https://{awslogs_region}.console.aws.amazon.com/cloudwatch/home?region={awslogs_region}#logEventViewer:group={awslogs_group};stream={awslogs_stream_name}"

    def monitor_job(self, context: Context):
        """Monitor an AWS Batch job.

        This can raise an exception or an AirflowTaskTimeout if the task was
        created with ``execution_timeout``.
        """
        if not self.job_id:
            raise AirflowException("AWS Batch job - job_id was not found")

        try:
            job_desc = self.hook.get_job_description(self.job_id)
            job_definition_arn = job_desc["jobDefinition"]
            job_queue_arn = job_desc["jobQueue"]
            self.log.info(
                "AWS Batch job (%s) Job Definition ARN: %r, Job Queue ARN: %r",
                self.job_id,
                job_definition_arn,
                job_queue_arn,
            )
        except KeyError:
            self.log.warning("AWS Batch job (%s) can't get Job Definition ARN and Job Queue ARN", self.job_id)
        else:
            BatchJobDefinitionLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                job_definition_arn=job_definition_arn,
            )
            BatchJobQueueLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                job_queue_arn=job_queue_arn,
            )

        if self.awslogs_enabled:
            if self.waiters:
                self.waiters.wait_for_job(self.job_id, get_batch_log_fetcher=self._get_batch_log_fetcher)
            else:
                self.hook.wait_for_job(self.job_id, get_batch_log_fetcher=self._get_batch_log_fetcher)
        else:
            if self.waiters:
                self.waiters.wait_for_job(self.job_id)
            else:
                self.hook.wait_for_job(self.job_id)

        awslogs = []
        try:
            awslogs = self.hook.get_job_all_awslogs_info(self.job_id)
        except AirflowException as ae:
            self.log.warning("Cannot determine where to find the AWS logs for this Batch job: %s", ae)

        if awslogs:
            self.log.info("AWS Batch job (%s) CloudWatch Events details found. Links to logs:", self.job_id)
            for log in awslogs:
                self.log.info(self._format_cloudwatch_link(**log))
            if len(awslogs) > 1:
                # there can be several log streams on multi-node jobs
                self.log.warning(
                    "out of all those logs, we can only link to one in the UI. Using the first one."
                )

            CloudWatchEventsLink.persist(
                context=context,
                operator=self,
                region_name=self.hook.conn_region_name,
                aws_partition=self.hook.conn_partition,
                **awslogs[0],
            )

        self.hook.check_job_success(self.job_id)
        self.log.info("AWS Batch job (%s) succeeded", self.job_id)

    def execute_complete(self, context: Context, event: Optional[dict[str, Any]] = None) -> str:
        """Execute when the trigger fires - fetch logs and complete the task."""
        # Call parent's execute_complete first
        job_id = super().execute_complete(context, event)
        
        # Only fetch logs if we're in deferrable mode and awslogs are enabled
        # In non-deferrable mode, logs are already fetched by monitor_job()
        if self.deferrable and self.awslogs_enabled and job_id:
            # Set job_id for our log fetching methods
            self.job_id = job_id
            
            # Get job logs and display them
            try:
                # Use the log fetcher to display container logs
                log_fetcher = self._get_batch_log_fetcher()
                if log_fetcher:
                    log_fetcher.get_all_logs()
            except Exception as e:
                self.log.warning("Could not fetch batch job logs: %s", e)
            
            # Get CloudWatch log links
            awslogs = []
            try:
                awslogs = self.hook.get_job_all_awslogs_info(self.job_id)
            except AirflowException as ae:
                self.log.warning("Cannot determine where to find the AWS logs for this Batch job: %s", ae)

            if awslogs:
                self.log.info("AWS Batch job (%s) CloudWatch Events details found. Links to logs:", self.job_id)
                for log in awslogs:
                    self.log.info(self._format_cloudwatch_link(**log))
                
                CloudWatchEventsLink.persist(
                    context=context,
                    operator=self,
                    region_name=self.hook.conn_region_name,
                    aws_partition=self.hook.conn_partition,
                    **awslogs[0],
                )
            
            self.log.info("AWS Batch job (%s) succeeded", self.job_id)
        
        return job_id
