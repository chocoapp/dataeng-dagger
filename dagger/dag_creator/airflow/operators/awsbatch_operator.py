from typing import Any, Optional, Union

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

    def _fetch_and_log_cloudwatch(self, context: Context, job_id: str) -> tuple[list[str], Optional[str]]:
        """
        Fetch CloudWatch logs for the given job_id, log them to Airflow,
        and return (last_logs, cloudwatch_link).
        """
        last_logs: list[str] = []
        cloudwatch_link: Optional[str] = None

        if self.awslogs_enabled:
            # Fetch last 50 log messages
            try:
                log_fetcher = self._get_batch_log_fetcher(job_id)
                if log_fetcher:
                    self.log.info("Fetching the latest 50 messages from CloudWatch:")
                    last_logs = log_fetcher.get_last_log_messages(50)
                    for message in last_logs:
                        self.log.info(message)
            except Exception as e:
                self.log.warning("Could not fetch batch job logs: %s", e)

            # Fetch CloudWatch log link
            try:
                awslogs = self.hook.get_job_all_awslogs_info(job_id)
            except AirflowException as ae:
                self.log.warning("Cannot determine where to find the AWS logs: %s", ae)
                awslogs = []
            else:
                if awslogs:
                    cloudwatch_link = self._format_cloudwatch_link(**awslogs[0])
                    self.log.info("AWS Batch job (%s) CloudWatch Events details found:", job_id)
                    for log in awslogs:
                        self.log.info(self._format_cloudwatch_link(**log))
                    CloudWatchEventsLink.persist(
                        context=context,
                        operator=self,
                        region_name=self.hook.conn_region_name,
                        aws_partition=self.hook.conn_partition,
                        **awslogs[0],
                    )

        return last_logs, cloudwatch_link

    def execute(self, context: Context) -> Union[str, None]:
        """Submit and monitor an AWS Batch job, including early failures."""
        # First call parent execute, which will submit the job and possibly defer
        result = super().execute(context)
        
        # If we reach here without exception, the task completed (didn't defer)
        return result

    def defer(self, *, trigger, method_name: str = "execute_complete", kwargs=None, timeout=None):
        """Override defer to store job_id in XCom before deferring."""
        # Store job_id in XCom so it's available when the task resumes
        if hasattr(self, 'job_id') and self.job_id:
            # Get task instance from current context
            from airflow.operators.python import get_current_context
            try:
                context = get_current_context()
                context['task_instance'].xcom_push(key='batch_job_id', value=self.job_id)
                self.log.info(f"Stored job_id in XCom before deferring: {self.job_id}")
            except Exception as e:
                self.log.warning(f"Could not store job_id in XCom: {e}")
        
        # Call parent defer method
        super().defer(trigger=trigger, method_name=method_name, kwargs=kwargs, timeout=timeout)

    def execute_complete(self, context: Context, event: Optional[dict[str, Any]] = None) -> str:
        """Execute when the trigger fires - fetch logs first, then check job status."""
        job_id = event.get("job_id") if event else None
        if not job_id:
            raise AirflowException("No job_id found in event data from trigger.")

        self.job_id = job_id

        # Always fetch logs before checking status
        last_logs, cloudwatch_link = self._fetch_and_log_cloudwatch(context, job_id)

        try:
            self.hook.check_job_success(job_id)
        except AirflowException as e:
            raise AirflowException(_format_extra_info(str(e), last_logs, cloudwatch_link))

        self.log.info("AWS Batch job (%s) succeeded", job_id)
        return job_id

    def resume_execution(self, next_method: str, next_kwargs: Optional[dict[str, Any]], context: Context):
        """Override resume_execution to handle trigger failures and fetch logs."""
        # Retrieve job_id from XCom if not available on the instance
        if not hasattr(self, 'job_id') or not self.job_id:
            task_instance = context.get('task_instance')
            if task_instance:
                try:
                    stored_job_id = task_instance.xcom_pull(task_ids=task_instance.task_id, key='batch_job_id')
                    if stored_job_id:
                        self.job_id = stored_job_id
                        self.log.info(f"Retrieved job_id from XCom: {stored_job_id}")
                except Exception as e:
                    self.log.debug(f"Could not retrieve job_id from XCom: {e}")
        
        try:
            return super().resume_execution(next_method, next_kwargs, context)
        except TaskDeferralError as e:
            # When trigger fails, try to fetch logs if job_id is available
            if hasattr(self, 'job_id') and self.job_id and self.awslogs_enabled:
                self.log.info("Batch job trigger failed - fetching CloudWatch logs...")
                last_logs, cloudwatch_link = self._fetch_and_log_cloudwatch(context, self.job_id)
                # Re-raise with enhanced error message including logs
                raise AirflowException(
                    _format_extra_info(f"Batch job {self.job_id} failed: {e}", last_logs, cloudwatch_link)
                )
            else:
                self.log.warning("Cannot fetch logs for failed batch job - job_id or awslogs_enabled not available")
            raise
