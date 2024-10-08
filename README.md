
TODO:
- How to import module
- How to write QA tasks
- Details about task failures etc
- Add a final kill to raise error
- talk about aliases and accessing them
- talks about external tasks

# Pipeline Class Documentation

## Overview

The `Pipeline` class is a comprehensive and versatile tool designed to manage and automate complex data processing workflows. It is ideal for environments where the integrity, accuracy, and sequential execution of tasks are critical. The primary purpose of the `Pipeline` is to facilitate the orchestration of a series of tasks, ensuring that each task is executed in the correct order and that dependencies between tasks are properly managed.

### Key Features:

1. **Task Management**: The `Pipeline` class allows you to define a sequence of tasks, including both regular and QA (Quality Assurance) tasks. Each task can be defined with specific queries, conditions for QA checks, and execution stages.

2. **Stage-Based Execution**: Tasks are organized into stages, with each stage representing a phase in the data processing workflow. The pipeline ensures that tasks within a stage are executed before moving on to the next stage, maintaining the logical flow of data processing.

3. **QA Integration**: The `Pipeline` integrates QA tasks that validate data at different stages of the pipeline. These QA tasks are designed to ensure that the data meets specified conditions before proceeding, helping to maintain data quality throughout the workflow.

4. **Logging and Monitoring**: The `Pipeline` class includes robust logging capabilities through the `FusionLogger` class. Logs can be directed to the console, files, or Slack channels, providing real-time monitoring and post-execution analysis of the pipeline's performance.

5. **Error Handling and Notifications**: The pipeline is equipped with detailed error handling mechanisms that capture and log errors, preventing the pipeline from proceeding in case of critical failures. Notifications can be sent via email, summarizing the execution status of the pipeline.

### Use Cases:

- **Data Aggregation and Transformation**: Automate the process of aggregating and transforming large datasets, preparing them for analysis or reporting. The pipeline ensures that each transformation step is completed successfully before moving to the next.

- **Data Validation**: Use QA tasks to validate the data at various stages, ensuring that it meets predefined conditions before being used in further processing or analysis.

- **Automated Reporting**: Automate the generation of reports by defining tasks that extract, transform, and load (ETL) data, followed by QA checks to ensure the accuracy of the final report.

- **Data Migration**: Facilitate the migration of data from one environment to another, using stages to manage the extraction, transformation, and loading of data in a controlled manner.

### Sample Usage
```python
```python
from fusion_utils.pipeline import Pipeline
from fusion_utils.task import Task

# CREATE PIPELINE INSTANCE
pipeline = Pipeline('PL_Aggregate_ISR_DMA', QA_flag=ctx['QA'])
pipeline.set_email_recipients('dylan.doyle@jdpa.com')

# CONSTRUCT PIPELINE
task1 = Task(name='TRANSCOUNTS', query_definition='SELECT COUNT(*) FROM table')

task2 = Task(name='ALL_FK', query_definition='SELECT ALL_FK FROM table')

pipeline.add_task(task1)
pipeline.add_task(task2)

pipeline.execute_all()

```
## Pipeline Class

### Initialization

```python
pipeline = Pipeline(name: str, QA_flag: Optional[str] = None)
```

**Parameters:**
- `name` (str): The name of the pipeline.
- `QA_flag` (Optional[str]): Flag to enable QA mode. If `None`, defaults to `True`.

### Methods

#### `set_email_recipients(recipients: Union[str, List[str]])`

Sets the email recipients for notifications.

**Parameters:**
- `recipients` (Union[str, List[str]]): A string or a list of email addresses.

**Usage Example:**
```python
pipeline.set_email_recipients('dylan.doyle@jdpa.com')
```

#### `add_task(task: Task)`

Adds a task to the pipeline.

**Parameters:**
- `task` (Task): The task to add.

**Usage Example:**
```python
task = Task(name='TASK_1', query_definition='SELECT * FROM dataset.table', table_alias='task_table')
pipeline.add_task(task)
```

#### `add_external_task(df: pd.DataFrame, temp_table_name: str)`

Adds an external task to the pipeline by loading a DataFrame into BigQuery.

**Parameters:**
- `df` (pd.DataFrame): DataFrame to load.
- `temp_table_name` (str): The name of the temporary table to create.

**Usage Example:**
```python
query_df = pd.DataFrame({'column': [1, 2, 3]})
pipeline.add_external_task(query_df, 'query_df')
```

#### `execute_all()`

Executes all tasks added to the pipeline in their respective stages.

**Usage Example:**
```python
pipeline.execute_all()
```

## Task Class

The `Task` class represents a unit of work within the pipeline. It contains the query definition, execution details, and any conditions that must be met for QA tasks.

### Task Class Initialization

```python
task = Task(name, query_definition, table_alias=None, query=None, is_qa=False, optional=False, stage=None, condition=None, include_html=False)
```

**Parameters:**
- `name` (str): The name of the task.
- `query_definition` (str): The SQL query definition for the task.
- `table_alias` (Optional[str]): Alias for the table.
- `query` (Optional[str]): Translated query.
- `is_qa` (bool): Flag indicating if this is a QA task.
- `optional` (bool): If `True`, the task can fail without halting the pipeline.
- `stage` (Optional[int]): The stage in which the task should be executed.
- `condition` (Optional[str]): A string representing a lambda function used for QA condition checking.
- `include_html` (bool): Whether to include an HTML representation of the dataframe in the logs.

### Methods

#### `define_query(query_definition: str)`

Sets the query definition for the task.

**Parameters:**
- `query_definition` (str): The SQL query definition for the task.

#### `define_table_alias(table_alias: str)`

Sets the table alias for the task.

**Parameters:**
- `table_alias` (str): Alias for the table.

#### `define_optional(optional: bool)`

Sets whether the task is optional.

**Parameters:**
- `optional` (bool): If `True`, the task is optional.

## FusionLogger Class

The `FusionLogger` class is responsible for logging pipeline execution details. It can log to the console, a file, and a memory stream, and can also send log messages to Slack.

### Initialization

```python
logger = FusionLogger(slack_bot_token=None, slack_channel=None)
```

**Parameters:**
- `slack_bot_token` (Optional[str]): Token for Slack bot.
- `slack_channel` (Optional[str]): Slack channel ID for logging.

### Methods

#### `log(message: str, level: str = 'info')`

Logs a message at the specified logging level.

**Parameters:**
- `message` (str): The message to log.
- `level` (str): The logging level (`'info'`, `'warning'`, `'error'`, `'critical'`).

**Usage Example:**
```python
logger.log('This is an info message')
```

#### `get_log_contents() -> str`

Returns the contents of the log stored in the memory stream.

**Returns:**
- `str`: The contents of the log.

#### `attach_to_email(email_message: MIMEMultipart)`

Attaches the log contents as a text file to an email.

**Parameters:**
- `email_message` (MIMEMultipart): The email message object.

### Example Usage

```python
task = Task(name='TRANSCOUNTS', query_definition='SELECT COUNT(*) FROM table', is_qa=True, condition="df['count'] == 100")
pipeline.add_task(task)
pipeline.execute_all()
```

## Additional Information

This documentation covers the essential methods and usage patterns of the `Pipeline`, `Task`, and `FusionLogger` classes. By following the examples provided, users can create, manage, and execute complex data pipelines with integrated QA checks and comprehensive logging.

**Note:** This is a high-level overview. For more advanced usage and configurations, please refer to the source code or additional detailed documentation.

## Best Practices
Tasks properties can by defined by Dictionaries or Arrays, then passed to a helper function to construct and add to the pipeline. This way, Task details can be generated externally in previous steps and cleanly passed to the Pipeline object without overcrowding the Pipeline object creation step.

```python
from fusion_utils.pipeline import Pipeline
from fusion_utils.task import Task

# CREATE PIPELINE INSTANCE
pipeline = Pipeline('PL_Aggregate_ISR_DMA', QA_flag=ctx['QA'])
pipeline.set_email_recipients('dylan.doyle@jdpa.com')

# CONSTRUCT PIPELINE
tasks = pyspark_utils.get_pandas_from_task_alias('queries').to_dict(orient='records')
qa_tasks = pyspark_utils.get_pandas_from_task_alias('qa_queries').to_dict(orient='records')

for task in tasks:
    pipeline.add_task(Task(**task))

for qa_task in qa_tasks:
    pipeline.add_task(Task(**qa_task))

pipeline.execute_all()
```

In this example, Task definitions are created in a previous AIC step and the resultant dataframe is then accessed to create the Task objects. 

The creation of the Task Dictionaries can look something like this:

```python
import pandas as pd
c = '''select * from table'''
WRITE_BRIDGE = '''select * from {STG_BRIDGE} where condition = False''

data = [{'name':'STG_BRIDGE', 'query_definition':STG_BRIDGE,'stage':1},{'name':'WRITE_BRIDGE', 'query_definition':WRITE_BRIDGE,'stage':2}]

return pd.DataFrame(data)
```

This will result in a DataFrame object containing various records and details of the tasks which can be ingested by the Pipeline class using a helper function.

#### External Tasks:


