import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import bigquery
import datetime
import uuid
import errors
from concurrent.futures import ThreadPoolExecutor, as_completed

class Pipeline:
    def __init__(self, name, QA_flag=None):
        self.name = name
        self.stages = []
        if QA_flag is None:
            print("NO QA_FLAG PASSED. DEFAULTING TO TRUE.")
            self.QA = True
        else:
            self.QA = QA_flag
        self.tasks = []
        self.client = bigquery.Client(project='aic-production-core')
        self.temp_tables = {}
        self.status = {}
        self.set_table_map()
        self.initialize_datasets()
        self.set_smtp_ip()

    def set_table_map(self, dataset='3349c7ea_09a2_461d_87f5_312a5401c51a', table='LKP_QA_TABLE_MAPPING'):
        table_map = f'`{dataset}.{table}`'
        self.table_map_df = self.client.query(f'SELECT * FROM {table_map}').to_dataframe()

    def set_email_recipients(self, recipients):
        if isinstance(recipients, str):
            self.recipients = [recipients]
        else:
            self.recipients = recipients

    def set_smtp_ip(self, dataset='3349c7ea_09a2_461d_87f5_312a5401c51a', table='LKP_SMTP_IP'):
        """Set the SMTP IP address from a BigQuery table."""
        if '_' not in dataset:
            dataset_id = self.translate_dataset(dataset)
            smtp_map = f'`{dataset_id}.{table}`'
        else:
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
            raise ValueError('Cannot retrieve SMTP server IP.') from e

    def translate_dataset(self, alias):
        return getattr(self, alias)

    def initialize_datasets(self):
        for _, row in self.table_map_df.iterrows():
            alias = row['alias']
            dataset = row['qa_dataset'] if self.QA else row['prod_dataset']
            setattr(self, alias, dataset)

    def send_email(self, subject, body, recipients=None):
        """
        Send an email using SMTP to multiple recipients.
        """
        if not hasattr(self, 'smtp_ip'):
            raise errors.SMTPConfigurationError("SMTP server IP is not configured.")

        if not recipients and not self.recipients:
            raise errors.SMTPConfigurationError('No recipient pass or defined within pipeline attributes. Either pass recipient string/list or use set_recipients to define at pipeline level.')

        if not recipients:
            recipients = self.recipients

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

        try:
            server = smtplib.SMTP(self.smtp_ip, 25)
            server.ehlo()  # Necessary for some SMTP servers
            server.sendmail(sender, recipients, message.as_string())
            server.quit()
            print("Email sent successfully.")
        except Exception as e:
            print("Failed to send email:", str(e))

    def run_query(self, query):
        return self.client.query(query).result()

    def add_task(self, task):
        self.tasks.append(task)
        temp_table = f"{self.client.project}.AIC_BRANCH_JOB.{task.table_alias}"
        task.temp_table = temp_table

    def execute_stage(self, stage_tasks):
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.execute_task, task): task for task in stage_tasks}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    future.result()
                except Exception as e:
                    if not task.optional:
                        raise e
                    else:
                        print(f"Optional task '{task.name}' generated an exception: {e}")

    def execute_all(self):
        """Execute all tasks in the pipeline stage by stage."""
        max_stage = max(task.stage for task in self.tasks)
        for stage in range(1, max_stage + 1):
            stage_tasks = [task for task in self.tasks if task.stage == stage]
            if stage_tasks:
                print(f"Executing stage {stage} with {len(stage_tasks)} tasks")
                self.execute_stage(stage_tasks)

    def execute_task(self, task):
        """Run a query and write the results to a temporary table in BigQuery."""
        if task not in self.tasks:
            raise errors.TaskError(f"Task '{task.name}' has not been added to the pipeline. Use Pipeline.add_task() to execute.")

        if not task.query_definition:
            if not task.optional:
                raise errors.TaskError(f'{task.name} does not have a defined query')
            else:
                print(f'WARNING: {task.name} does not have a defined query. Skipping optional task...')
                self.update_status(task, 'Failed: No query defined')
                return

        self.update_status(task, 'Started')

        # Generate the actual query using the pipeline's temp tables
        task.translate_query(self)

        try:
            self.write_to_temp_table(task.query, task.temp_table)
            self.update_temp_table_list(task.name, task.temp_table)
            self.update_status(task, 'Completed')
            print(f"{task.name} Completed.")
        except Exception as e:
            self.update_status(task, f'Failed: {str(e)}')
            if not task.optional:
                raise e

    def execute_task_by_name(self, task_name):
        """Find a task by name and execute it."""
        task = next((t for t in self.tasks if t.name == task_name), None)
        if task is None:
            raise errors.TaskError(f"No task found with name '{task_name}'")
        self.execute_task(task)

    def execute_stage_by_number(self, stage_number):
        """Execute all tasks in a given stage number."""
        stage_tasks = [task for task in self.tasks if task.stage == stage_number]
        if not stage_tasks:
            raise errors.TaskError(f"No tasks found for stage {stage_number}")
        self.execute_stage(stage_tasks)

    def update_temp_table_list(self, task_name, table):
        self.temp_tables[task_name] = f'{table}'

    def update_status(self, task, status):
        update = {
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': status
        }

        if status == 'Completed':
            update['success'] = True
        elif status.startswith('Failed'):
            update['success'] = False

        if task.name not in self.status:
            self.status[task.name] = []
        self.status[task.name].append(update)

    def write_to_temp_table(self, query, temp_table_name):
        """Run a query and write the results to a temporary table in BigQuery."""
        job_config = bigquery.QueryJobConfig(destination=temp_table_name, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

        try:
            # Start the query, passing in the extra configuration.
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for the query to complete

            # Set the expiration time on the destination table.
            table = self.client.get_table(query_job.destination)
            table.expires = datetime.datetime.now() + datetime.timedelta(hours=1)
            self.client.update_table(table, ["expires"])  # API request

            print(f"Temporary table {temp_table_name} created.")
            return query_job.destination
        except Exception as e:
            print(f"Failed to create temporary table {temp_table_name}: {str(e)}")
            raise e

class Task:
    current_stage = 1

    def __init__(self, name, query_definition=None, table_alias=None, optional=False, stage=None, **kwargs):
        self.name = name
        self.optional = optional
        self.kwargs = kwargs
        self.query_definition = query_definition
        self.table_alias = uuid.uuid4().hex if not table_alias else table_alias
        if stage is None:
            self.stage = Task.current_stage
            Task.current_stage += 1
        else:
            self.stage = stage
        self.query = None

    def define_query(self, query_definition):
        self.query_definition = query_definition

    def define_table_alias(self, table_alias):
        self.table_alias = table_alias

    def define_optional(self, optional):
        self.optional = optional

    def translate_query(self, pipeline):
        """Generate the actual query by replacing placeholders using pipeline temp table names"""
        if self.query_definition:
            self.query = self.query_definition.format(**pipeline.temp_tables)
        else:
            self.query = None

# Example usage
if __name__ == "__main__":
    pipeline = Pipeline('Fusion_test')
    query = "SELECT * FROM `some_dataset.some_table`"
    task1 = Task('TEST', query, 'ISR_STG', optional=True)

    # Define a task definition function
    def task_definition(pipeline, **kwargs):
        pipeline.execute_task(kwargs['task'])

    # Set the task definition
    task1.define_query(task_definition)

    # Add task to the pipeline
    pipeline.add_task(task1)

    # Execute a specific task by name
    pipeline.execute_task_by_name('TEST')

    # Execute all tasks in a specific stage by stage number
    pipeline.execute_stage_by_number(1)

    # Execute all tasks in the pipeline
    pipeline.execute_all()
    print(pipeline.status)  # Print the status to see the task execution details
