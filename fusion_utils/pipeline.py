import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import bigquery
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Union
import time
import threading
import pandas as pd
import numpy as np
import re
from .task import Task
from .errors import TaskError, SMTPConfigurationError
from .fusion_logger import FusionLogger


class Pipeline:
    def __init__(self, name: str, qa_flag: Optional[str] = None):
        """
        Initialize the Pipeline.

        Args:
            name (str): The name of the pipeline.
            qa_flag (str, optional): The QA flag. Defaults to None.
        """
        self.name = name
        self.client = bigquery.Client(project='aic-production-core')
        self.set_qa_flag(qa_flag)
        self._set_table_map()
        self._initialize_datasets()
        self._set_smtp_ip()
        self.halt_execution = False
        self.stages = []
        self.errors = []
        self.qa_queries = []
        self.tasks = []
        self.temp_tables = {}
        self.status = {}
        self.task_execution_times = {}
        self.failed_tasks = []
        self.logger = self._create_logger()

    def _get_log_channel(self, dataset='25ba15f5_2d68_4098_96bc_37cc8936c061', table='LKP_LOG_CHANNELS') -> str:
        """
        Retrieve the logging channel for the pipeline from the BigQuery table.

        Args:
            dataset (str): The BigQuery dataset containing the log channel information.
            table (str): The table within the dataset.

        Returns:
            str: The logging channel ID.
        """
        table_map = f'`{dataset}.{table}`'
        self.table_map_df = self.client.query(f'SELECT * FROM {table_map}').to_dataframe()
        channel = self.table_map_df.loc[self.table_map_df['pipeline'] == self.name, 'qa_channel' if self.qa_flag else 'prod_channel']

        if channel.empty:
            channel = self.table_map_df.loc[self.table_map_df['pipeline'] == 'ALL', 'qa_channel' if self.qa_flag else 'prod_channel']

        return channel.iloc[0]

    def _create_logger(self) -> Optional[FusionLogger]:
        """
        Create and return a FusionLogger instance for logging.

        Returns:
            Optional[FusionLogger]: FusionLogger instance or None if creation fails.
        """
        try:
            slack_bot_token = self.translate_tables('SELECT * FROM `$Bronze:LKP_SLACK_TOKEN`')
            slack_bot_token = self.client.query(slack_bot_token).result().to_dataframe().iloc[0, 0]
            print('Logger Created!')
            return FusionLogger(slack_bot_token, self._get_log_channel())
        except Exception as e:
            print(f"Failed to create slack logger: {e}")
            return None

    def set_qa_flag(self, qa_flag: Optional[str]):
        """
        Set the QA flag for the pipeline.

        Args:
            qa_flag (str, optional): The QA flag. If None, defaults to True.
        """
        if qa_flag is None:
            self.logger.log("NO QA_FLAG PASSED. DEFAULTING TO TRUE.")
            self.qa_flag = True
        elif isinstance(qa_flag, str):
            self.qa_flag = qa_flag.lower() == 'true'
        else:
            self.qa_flag = qa_flag

    def _print_initial_summary(self):
        """
        Print the initial summary of the pipeline, including task and stage details.
        """
        summary = f"""
        Initial Pipeline Summary
        ========================
        Pipeline Name: {self.name}
        QA Mode: {'Enabled' if self.qa_flag else 'Disabled'}
        Total Tasks: {len(self.tasks)}
        Total Stages: {len(set(task.stage for task in self.tasks))}
        Tasks by Stage:
        """
        stages = {}
        for task in self.tasks:
            stages.setdefault(task.stage, []).append(task)

        for stage, tasks in sorted(stages.items()):
            stage_label = 'QA' if stage == 999 else stage
            summary += f"  Stage {stage_label}:\n"
            for task in tasks:
                summary += f"    - {task.name}\n"

        self.logger.log(summary.strip())

    def _set_table_map(self, dataset='3349c7ea_09a2_461d_87f5_312a5401c51a', table='LKP_QA_TABLE_MAPPING'):
        """
        Set the table map for datasets used in the pipeline.

        Args:
            dataset (str): The dataset name containing the table mappings.
            table (str): The table name within the dataset.
        """
        table_map = f'`{dataset}.{table}`'
        self.table_map_df = self.client.query(f'SELECT * FROM {table_map}').to_dataframe()
        self.dataset_map = dict(zip(self.table_map_df['alias'], self.table_map_df['qa_dataset' if self.qa_flag else 'prod_dataset']))

    def set_email_recipients(self, recipients: Union[str, List[str]]):
        """
        Set the email recipients for notifications.

        Args:
            recipients (Union[str, List[str]]): A string or a list of email addresses.
        """
        self.recipients = [recipients] if isinstance(recipients, str) else recipients

    def _set_smtp_ip(self, dataset='3349c7ea_09a2_461d_87f5_312a5401c51a', table='LKP_SMTP_IP'):
        """
        Set the SMTP IP address for sending emails.

        Args:
            dataset (str): The dataset containing the SMTP IP information.
            table (str): The table name within the dataset.

        Raises:
            SMTPConfigurationError: If the SMTP IP address cannot be retrieved or is invalid.
        """
        smtp_map = f'`{dataset}.{table}`'
        try:
            query = f"SELECT ip FROM {smtp_map} LIMIT 1"
            results = self.client.query(query).result()
            for row in results:
                self.smtp_ip = row['ip']
                break
            if not isinstance(self.smtp_ip, str):
                raise ValueError("Retrieved SMTP IP is not a string.")
        except Exception as e:
            raise SMTPConfigurationError('Cannot retrieve SMTP server IP.') from e

    def _initialize_datasets(self):
        """
        Initialize the datasets for the pipeline based on the table map.
        """
        for _, row in self.table_map_df.iterrows():
            alias = row['alias']
            dataset = row['qa_dataset'] if self.qa_flag else row['prod_dataset']
            setattr(self, alias, dataset)

    def translate_tables(self, query: str) -> str:
        """
        Translate dataset aliases in a query to their actual dataset IDs.

        Args:
            query (str): The SQL query with dataset aliases.

        Returns:
            str: The translated SQL query.
        """
        for alias, dataset_id in self.dataset_map.items():
            query = query.replace(f'${alias}:', f'{dataset_id}.')
        return query

    def translate_query(self, query: str) -> str:
        """
        Translate both dataset aliases and temporary table placeholders in a query.

        Args:
            query (str): The SQL query with placeholders.

        Returns:
            str: The fully translated SQL query.

        Raises:
            TaskError: If there are unresolved placeholders in the query.
        """
        query = self.translate_tables(query)
        for alias, table_name in self.temp_tables.items():
            query = query.replace(f'${{{alias}}}', table_name)

        unresolved_placeholders = re.findall(r'\$\{.*?\}', query)
        if unresolved_placeholders:
            error_message = f"Placeholder(s) could not be translated to BQ table or task: {', '.join(unresolved_placeholders)}"
            self.logger.log(f"Final query with unresolved placeholders: {query}", 'debug')
            self.logger.log(error_message, 'error')
            raise TaskError(error_message)

        return query

    def _print_elapsed_time(self, start_time: float, stop_event: threading.Event):
        """
        Continuously log the elapsed time during a long-running operation.

        Args:
            start_time (float): The start time of the operation.
            stop_event (threading.Event): An event that stops the loop when set.
        """
        while not stop_event.is_set():
            elapsed_time = time.time() - start_time
            self.logger.log(f"Elapsed time: {elapsed_time:.2f} seconds", 'info')
            time.sleep(10)  # Adjust the sleep interval as needed

    def _send_email(self, subject: str, body: str, recipients: Optional[Union[str, List[str]]] = None):
        """
        Send an email with the specified subject and body.

        Args:
            subject (str): The subject of the email.
            body (str): The HTML body of the email.
            recipients (Union[str, List[str]], optional): A single email address or a list of addresses. Defaults to None.

        Raises:
            SMTPConfigurationError: If the SMTP server IP is not configured or recipients are not specified.
        """
        if not hasattr(self, 'smtp_ip'):
            raise SMTPConfigurationError("SMTP server IP is not configured.")

        if not recipients:
            if hasattr(self, 'recipients'):
                recipients = self.recipients
            else:
                raise SMTPConfigurationError('No recipient passed or defined within pipeline attributes.')

        if isinstance(recipients, str):
            recipients = [recipients]

        sender = 'pinapps@jdpa.com'
        message = MIMEMultipart()
        message['From'] = sender
        message['To'] = ", ".join(recipients)
        message['Subject'] = subject
        css = '<style>.pass { color: #008000; } .fail { color: #FF0000; }</style>'
        body_html = f"<html><head>{css}</head><body>{body}</body></html>"
        message.attach(MIMEText(body_html, 'html'))

        self.logger.attach_to_email(message)

        try:
            server = smtplib.SMTP(self.smtp_ip, 25)
            server.ehlo()
            server.sendmail(sender, recipients, message.as_string())
            server.quit()
        except Exception as e:
            self.logger.log(f"Failed to send email: {str(e)}", 'error')

    def _execute_query(self, query: str, temp_table_name: Optional[str] = None):
        """
        Execute a SQL query and optionally store the results in a temporary table.

        Args:
            query (str): The SQL query to execute.
            temp_table_name (str, optional): The name of the temporary table to store results. Defaults to None.

        Returns:
            bigquery.QueryJob: The BigQuery query job.
            float: The time taken to execute the query.

        Raises:
            TaskError: If the query execution fails.
        """
        query = self.translate_query(query)
        elapsed_time_thread = None
        stop_event = threading.Event()

        try:
            start_time = time.time()
            elapsed_time_thread = threading.Thread(target=self._print_elapsed_time, args=(start_time, stop_event))
            elapsed_time_thread.start()

            ddl_patterns = [
                r"create\s+table", r"execute\s", r"replace\s+table", r"insert\s+into", r"drop\s+table",
                r"alter\s+table", r"truncate\s+table", r"\bupdate\b", r"delete\s+from"
            ]

            is_ddl = any(re.search(pattern, query.lower()) for pattern in ddl_patterns)

            if temp_table_name and not is_ddl:
                job_config = bigquery.QueryJobConfig(
                    destination=temp_table_name,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                )
                query_job = self.client.query(query, job_config=job_config)
            else:
                query_job = self.client.query(query)

            query_job.result()

            stop_event.set()
            elapsed_time_thread.join()

            total_elapsed_time = time.time() - start_time
            self.logger.log(f"\nQuery executed in {total_elapsed_time:.2f} seconds.")

            if not is_ddl:
                self._create_temp_table(query_job, temp_table_name)
                self._update_temp_table_list(query, temp_table_name)

            return query_job, total_elapsed_time

        except Exception as e:
            if elapsed_time_thread:
                stop_event.set()
                elapsed_time_thread.join()
            error_message = str(e).split('\n\n')[0].strip()
            self.logger.log(f"Failed to execute query: {error_message}", 'error')
            self.logger.log(f"Query: {query}", 'debug')
            raise TaskError(f"Failed to execute query: {error_message}")

    def _create_temp_table(self, query_job: bigquery.QueryJob, temp_table_name: Optional[str]) -> Optional[str]:
        """
        Create a temporary table based on the query job result.

        Args:
            query_job (bigquery.QueryJob): The query job object.
            temp_table_name (str, optional): The temporary table name.

        Returns:
            Optional[str]: The temporary table name, or None if creation fails.

        Raises:
            TaskError: If the temporary table creation fails.
        """
        try:
            destination = query_job.destination
            if destination is None:
                self.logger.log(f"No destination table created for job: {query_job.job_id}")
                return None

            table = self.client.get_table(destination)
            if table.table_type != 'TEMPORARY':
                table.expires = datetime.datetime.now() + datetime.timedelta(hours=1)
                self.client.update_table(table, ["expires"])
                self.logger.log(f"Temporary table {temp_table_name} created.")
            else:
                self.logger.log(f"Skipping expiration update for anonymous table {destination}")
            return temp_table_name
        except Exception as e:
            error_message = str(e).split('\n\n')[0].strip()
            self.logger.log(f"Failed to create temporary table {temp_table_name}: {error_message}", 'error')
            raise TaskError(f"Failed to create temporary table {temp_table_name}: {error_message}")

    def _log_task_execution(self, task_name: str, data_size: int, elapsed_time: float):
        """
        Log task execution details to BigQuery.

        Args:
            task_name (str): The name of the task.
            data_size (int): The size of the data processed.
            elapsed_time (float): The time taken to execute the task.
        """
        try:
            if "test" in task_name.lower():
                self.logger.log(f"Skipping logging for test task: {task_name}")
                return

            log_query = f"""
            INSERT INTO `{self.translate_dataset('Silver')}.LKP_TASK_LOG` (task_name, data_size, date_run, elapsed_time)
            VALUES ('{task_name}', {data_size}, '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', {elapsed_time})
            """
            self.client.query(log_query).result()
            self.logger.log(f"Logged task execution for task: {task_name}")
        except Exception as e:
            self.logger.log(f"Failed to log task execution for task: {task_name}. Error: {str(e)}", 'error')

    def add_task(self, task: Task):
        """
        Add a task to the pipeline.

        Args:
            task (Task): The task to add.
        """
        self.tasks.append(task)
        task.temp_table = None if task.is_qa else f"{self.client.project}.AIC_BRANCH_JOB.{task.table_alias}"

    def add_external_task(self, df: pd.DataFrame, temp_table_name: str):
        """
        Add an external task to the pipeline by loading a DataFrame into BigQuery.

        Args:
            df (pd.DataFrame): DataFrame to load.
            temp_table_name (str): The name of the temporary table to create.
        """
        dataset_id = "AIC_BRANCH_JOB"
        table_ref = f"{self.client.project}.{dataset_id}.{temp_table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        load_job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result()

        query_job = type('QueryJob', (object,), {'destination': table_ref})

        self._create_temp_table(query_job, temp_table_name)
        self.temp_tables[temp_table_name] = table_ref

    def _execute_stage(self, stage_tasks: List[Task]) -> float:
        """
        Execute all tasks within a given stage.

        Args:
            stage_tasks (List[Task]): Tasks to execute.

        Returns:
            float: Time taken to execute the stage.
        """
        stage_start_time = time.time()
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self._execute_task, task): task for task in stage_tasks}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    future.result()
                except Exception as e:
                    error_message = f"Task '{task.name}' in stage {task.stage} failed with exception: {e}"
                    self.errors.append(error_message)
                    self.logger.log(error_message, 'error')
                    self.failed_tasks.append(task)
                    if not task.optional:
                        self.halt_execution = True
                        return 0.0
        return time.time() - stage_start_time

    def _execute_task(self, task: Task):
        """
        Execute a single task.

        Args:
            task (Task): The task to execute.
        """
        start_time = time.time()
        try:
            if task.is_qa:
                self._execute_qa_task(task)
            else:
                self._execute_regular_task(task)
            self.task_execution_times[task.name] = time.time() - start_time
        except Exception as e:
            error_message = f"Task '{task.name}' in stage {task.stage} failed with exception: {e}"
            self.errors.append(error_message)
            self.logger.log(error_message, 'error')
            self._update_status(task, f"Failed with exception: {str(e)}")
            if not task.optional:
                self.halt_execution = True

    def _execute_regular_task(self, task: Task):
        """
        Execute a regular (non-QA) task in the pipeline.

        Args:
            task (Task): The task to execute.

        Raises:
            TaskError: If the task execution fails.
        """
        try:
            if task not in self.tasks:
                raise TaskError(f"Task '{task.name}' has not been added to the pipeline. Use Pipeline.add_task() to execute.")

            if not task.query_definition:
                if not task.optional:
                    raise TaskError(f'{task.name} does not have a defined query')
                else:
                    self.logger.log(f'WARNING: {task.name} does not have a defined query. Skipping optional task...', 'warning')
                    self._update_status(task, 'Failed: No query defined')
                    return

            self._update_status(task, 'Started')

            try:
                task.query = self.translate_query(task.query_definition)
            except KeyError as e:
                raise TaskError(f"Failed to translate query for task '{task.name}': Missing key {str(e)} in temp tables")

            estimated_data_size = self._estimate_data_size(task.query)
            self.logger.log(f"Estimated data size for task '{task.name}': {estimated_data_size/1000000000:.2f} Gb")

            estimated_run_time = self._estimate_run_time(estimated_data_size, task.name)
            if estimated_run_time:
                self.logger.log(f"Estimated run time for task '{task.name}': {estimated_run_time:.2f} seconds")

            try:
                temp_table_name = f"{self.client.project}.AIC_BRANCH_JOB.{task.table_alias}"
                query_job, elapsed_time = self._execute_query(task.query, temp_table_name)
                self._update_temp_table_list(task.name, temp_table_name)
                self._log_task_execution(task.name, estimated_data_size, elapsed_time)
                self._update_status(task, 'Completed')
                self.logger.log(f"{task.name} Completed.")
            except Exception as e:
                self._update_status(task, f'Failed: {str(e)}')
                self.logger.log(f"Task '{task.name}' failed with exception: {e}\nQuery: {task.query}", 'error')
                self._log_task_details(task)  # Log task details at the beginning
                if not task.optional:
                    raise e

        except Exception as e:
            error_message = f"Task '{task.name}' in stage {task.stage} failed with exception: {e}\nQuery: {task.query}"
            self.errors.append(error_message)
            self.logger.log(error_message, 'error')
            self._update_status(task, f"Failed with exception: {str(e)}")
            if not task.optional:
                self.halt_execution = True

    def _execute_qa_task(self, task: Task):
        """
        Execute a QA task in the pipeline.

        Args:
            task (Task): The QA task to execute.

        Raises:
            TaskError: If the QA task execution fails.
        """
        self.logger.log(f"Executing QA task '{task.name}'")
        try:
            query = self.translate_query(task.query_definition)
            query_job = self.client.query(query)
            result = query_job.result().to_dataframe()

            if not callable(task.condition):
                raise TaskError(f"Condition for task '{task.name}' is not callable")

            condition_result = task.condition(result)

            if condition_result:
                self.logger.log(f"QA check passed for task '{task.name}'")
                self._update_status(task, 'Completed')
            else:
                self.logger.log(f"QA check failed for task '{task.name}'")
                if task.optional:
                    self._update_status(task, 'Failed (Optional): QA check did not pass')
                else:
                    self._update_status(task, 'Failed: QA check did not pass')

            if task.include_html:
                task.html_result = result.to_html()

        except Exception as e:
            if task.optional:
                self._update_status(task, f'Failed (Optional): {str(e)}')
            else:
                self._update_status(task, f'Failed: {str(e)}')
            self.logger.log(f"QA task '{task.name}' failed with exception: {str(e)}", 'error')

    def execute_task_by_name(self, task_name: str):
        """
        Execute a task by its name.

        Args:
            task_name (str): The name of the task to execute.

        Raises:
            TaskError: If no task with the specified name is found.
        """
        task = next((t for t in self.tasks if t.name == task_name), None)
        if task is None:
            raise TaskError(f"No task found with name '{task_name}'")
        self._execute_task(task)

    def _update_temp_table_list(self, task_name: str, table: str):
        """
        Update the list of temporary tables with the given task and table name.

        Args:
            task_name (str): The name of the task.
            table (str): The temporary table name.
        """
        self.temp_tables[task_name] = table

    def _update_status(self, task: Task, status: str):
        """
        Update the execution status of a task.

        Args:
            task (Task): The task to update.
            status (str): The new status of the task.
        """
        update = {'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'status': status}
        update['success'] = status == 'Completed' or not status.startswith('Failed')

        if task.name not in self.status:
            self.status[task.name] = []
        self.status[task.name].append(update)

    def _estimate_data_size(self, query: str) -> int:
        """
        Estimate the data size of a query.

        Args:
            query (str): The SQL query to estimate.

        Returns:
            int: The estimated data size in bytes.
        """
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = self.client.query(query, job_config=job_config)
        return query_job.total_bytes_processed

    def _get_historical_data(self) -> pd.DataFrame:
        """
        Retrieve historical task execution data from BigQuery.

        Returns:
            pd.DataFrame: DataFrame containing historical task execution data.
        """
        query = f"""
        SELECT task_name, data_size, elapsed_time
        FROM `{self.translate_dataset('Silver')}.LKP_TASK_LOG`
        """
        return self.client.query(query).to_dataframe()

    def _estimate_run_time(self, estimated_data_size: int, task_name: str) -> Optional[float]:
        """
        Estimate the run time for a task based on historical data.

        Args:
            estimated_data_size (int): The estimated data size in bytes.
            task_name (str): The name of the task.

        Returns:
            Optional[float]: The estimated run time in seconds, or None if no data is available.
        """
        df = self._get_historical_data()
        df_task = df[df['task_name'] == task_name]

        if df_task.empty:
            self.logger.log(f"No historical data available to estimate run time for task '{task_name}'", 'warning')
            return None

        x = df_task['data_size'].values
        y = df_task['elapsed_time'].values
        estimated_time = np.interp(estimated_data_size, x, y)
        return estimated_time

    def _send_completion_email(self, total_execution_time: float):
        """
        Send a completion email with a summary of the pipeline execution.

        Args:
            total_execution_time (float): The total execution time of the pipeline.
        """
        regular_status_summary = []
        qa_status_summary = []
        pass_flag = True

        for task_name, updates in self.status.items():
            latest_update = updates[-1]
            execution_time = self.task_execution_times.get(task_name, 'N/A')
            execution_time_str = f"{execution_time:.2f} seconds" if isinstance(execution_time, (int, float)) else execution_time
            task_summary = f"<span class='{ 'pass' if latest_update.get('success', False) else 'fail' }'>Task '{task_name}': {latest_update['status']}. (Execution Time: {execution_time_str})</span>"
            task = next((t for t in self.tasks if t.name == task_name), None)
            if task and task.is_qa:
                qa_status_summary.append(task_summary)
                if hasattr(task, 'html_result'):
                    qa_status_summary.append(task.html_result)
            else:
                regular_status_summary.append(task_summary)
            if not latest_update.get('success', False) and not latest_update['status'].startswith('Failed (Optional)'):
                pass_flag = False

        summary = "<br>".join(regular_status_summary)
        qa_summary = "<br>".join(qa_status_summary)
        flag_text = "PASS" if pass_flag else "FAIL"
        total_execution_time_str = f"{total_execution_time:.2f} seconds" if total_execution_time else "N/A"
        run_type = 'QA' if self.qa_flag else 'PROD'
        subject = f"{run_type}: {flag_text}: {self.name}"
        body = f"""
        <html>
            <head>
                <style>
                    .pass {{ color: #008000; }}
                    .fail {{ color: #FF0000; }}
                    .header {{ font-weight: bold; font-size: 16px; }}
                    .section {{ margin-bottom: 20px; }}
                    .summary {{ margin-left: 20px; }}
                </style>
            </head>
            <body>
                <div class="section">
                    <div class="header">Pipeline {self.name} Execution Status: {flag_text}</div>
                    <div class="summary">Total Execution Time: {total_execution_time_str}</div>
                </div>
                <div class="section">
                    <div class="header">Regular Tasks:</div>
                    <div class="summary">{summary}</div>
                </div>
                <div class="section">
                    <div class="header">QA Tasks:</div>
                    <div class="summary">{qa_summary}</div>
                </div>
            </body>
        </html>
        """

        try:
            self._send_email(subject, body)
            self.logger.log("Completion email sent successfully.")
        except Exception as e:
            self.logger.log(f"Failed to send completion email: {e}", 'error')
