from typing import Any, Optional

from airflow.exceptions import AirflowException, TaskDeferralError
from airflow.providers.amazon.aws.links.batch import (
    BatchJobDefinitionLink,
    BatchJobQueueLink,
)
from airflow.providers.amazon.aws.links.logs import CloudWatchEventsLink
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.utils.context import Context


def _format_extra_info(error_msg: str, last_logs: list[str], cloudwatch_link: Optional[str]) -> str:
    """Format the enhanced error message with logs and link."""
    extra_info = []
    if cloudwatch_link:
        extra_info.append(f"CloudWatch Logs: {cloudwatch_link}")
    if last_logs:
        extra_info.append("Last log lines:\n" + "\n".join(last_logs[-5:]))
    if extra_info:
        return f"{error_msg}\n\n" + "\n".join(extra_info)
    return error_msg


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

    def _fetch_and_log_cloudwatch(self, job_id: str) -> tuple[list[str], Optional[str]]:
        """Fetch CloudWatch logs for the given job_id and return (last_logs, cloudwatch_link)."""
        last_logs: list[str] = []
        cloudwatch_link: Optional[str] = None

        if not self.awslogs_enabled:
            return last_logs, cloudwatch_link

        # Fetch last log messages
        try:
            log_fetcher = self._get_batch_log_fetcher(job_id)
            if log_fetcher:
                last_logs = log_fetcher.get_last_log_messages(50)
                if last_logs:
                    self.log.info("CloudWatch logs (last 50 messages):")
                    for message in last_logs:
                        self.log.info(message)
        except Exception as e:
            self.log.warning("Could not fetch batch job logs: %s", e)

        # Get CloudWatch log link
        try:
            awslogs = self.hook.get_job_all_awslogs_info(job_id)
            if awslogs:
                cloudwatch_link = self._format_cloudwatch_link(**awslogs[0])
                self.log.info("CloudWatch link: %s", cloudwatch_link)
        except AirflowException as e:
            self.log.warning("Cannot determine CloudWatch log link: %s", e)

        return last_logs, cloudwatch_link

    def execute_complete(self, context: Context, event: Optional[dict[str, Any]] = None) -> str:
        """Execute when the trigger fires - fetch logs first, then check job status."""
        job_id = event.get("job_id") if event else None
        if not job_id:
            raise AirflowException("No job_id found in event data from trigger.")

        self.job_id = job_id

        # Always fetch logs before checking status
        last_logs, cloudwatch_link = self._fetch_and_log_cloudwatch(job_id)

        try:
            self.hook.check_job_success(job_id)
        except AirflowException as e:
            raise AirflowException(_format_extra_info(str(e), last_logs, cloudwatch_link))

        self.log.info("AWS Batch job (%s) succeeded", job_id)
        return job_id

    def resume_execution(self, next_method: str, next_kwargs: Optional[dict[str, Any]], context: Context):
        """Override resume_execution to handle trigger failures and fetch logs."""
        # Retrieve job_id from batch_job_details XCom if not available on the instance
        if not hasattr(self, 'job_id') or not self.job_id:
            task_instance = context.get('task_instance')
            if task_instance:
                try:
                    batch_job_details = task_instance.xcom_pull(task_ids=task_instance.task_id, key='batch_job_details')
                    if batch_job_details and 'job_id' in batch_job_details:
                        self.job_id = batch_job_details['job_id']
                        self.log.info(f"Retrieved job_id from batch_job_details XCom: {self.job_id}")
                except Exception as e:
                    self.log.debug(f"Could not retrieve job_id from batch_job_details XCom: {e}")
        
        try:
            return super().resume_execution(next_method, next_kwargs, context)
        except TaskDeferralError as e:
            # When trigger fails, try to fetch logs if job_id is available
            if hasattr(self, 'job_id') and self.job_id and self.awslogs_enabled:
                self.log.info("Batch job trigger failed - fetching CloudWatch logs...")
                last_logs, cloudwatch_link = self._fetch_and_log_cloudwatch(self.job_id)
                # Re-raise with enhanced error message including logs
                raise AirflowException(
                    _format_extra_info(f"Batch job {self.job_id} failed: {e}", last_logs, cloudwatch_link)
                )
            else:
                self.log.warning("Cannot fetch logs for failed batch job - job_id or awslogs_enabled not available")
            raise
