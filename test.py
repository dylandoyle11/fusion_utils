import pytest
from unittest.mock import patch, MagicMock
from Pipeline import Pipeline, Task
import my_pipeline.errors as errors

@pytest.fixture
@patch('my_pipeline.pipeline.bigquery.Client')
def pipeline(MockBigQueryClient):
    mock_client = MockBigQueryClient.return_value
    mock_client.query.return_value.result.return_value = []
    return Pipeline('TestPipeline')

def test_add_task(pipeline):
    task = Task('Task1', query_definition="SELECT * FROM `table`")
    pipeline.add_task(task)
    assert task in pipeline.tasks
    assert task.temp_table == f"{pipeline.client.project}.AIC_BRANCH_JOB.{task.table_alias}"

def test_execute_task(pipeline):
    task = Task('Task1', query_definition="SELECT * FROM `table`")
    pipeline.add_task(task)
    with patch.object(pipeline, 'write_to_temp_table') as mock_write:
        pipeline.execute_task(task)
        mock_write.assert_called_once_with(task.query, task.temp_table)
    assert pipeline.status[task.name][-1]['status'] == 'Completed'

def test_execute_optional_task_with_no_query(pipeline):
    task = Task('Task1', optional=True)
    pipeline.add_task(task)
    with patch('my_pipeline.pipeline.logging.warning') as mock_warning:
        pipeline.execute_task(task)
        mock_warning.assert_called_once_with(f'WARNING: {task.name} does not have a defined query. Skipping optional task...')
    assert pipeline.status[task.name][-1]['status'] == 'Failed: No query defined'

def test_execute_non_optional_task_with_no_query(pipeline):
    task = Task('Task1')
    pipeline.add_task(task)
    with pytest.raises(errors.TaskError):
        pipeline.execute_task(task)

def test_execute_stage(pipeline):
    task1 = Task('Task1', query_definition="SELECT * FROM `table1`", stage=1)
    task2 = Task('Task2', query_definition="SELECT * FROM `table2`", stage=1, optional=True)
    pipeline.add_task(task1)
    pipeline.add_task(task2)
    with patch.object(pipeline, 'write_to_temp_table') as mock_write:
        pipeline.execute_stage([task1, task2])
        mock_write.assert_any_call(task1.query, task1.temp_table)
        mock_write.assert_any_call(task2.query, task2.temp_table)

def test_execute_all(pipeline):
    task1 = Task('Task1', query_definition="SELECT * FROM `table1`", stage=1)
    task2 = Task('Task2', query_definition="SELECT * FROM `table2`", stage=2)
    pipeline.add_task(task1)
    pipeline.add_task(task2)
    with patch.object(pipeline, 'execute_stage') as mock_execute_stage:
        pipeline.execute_all()
        mock_execute_stage.assert_any_call([task1])
        mock_execute_stage.assert_any_call([task2])

def test_execute_task_by_name(pipeline):
    task = Task('Task1', query_definition="SELECT * FROM `table`")
    pipeline.add_task(task)
    with patch.object(pipeline, 'execute_task') as mock_execute:
        pipeline.execute_task_by_name('Task1')
        mock_execute.assert_called_once_with(task)

def test_execute_stage_by_number(pipeline):
    task1 = Task('Task1', query_definition="SELECT * FROM `table1`", stage=1)
    task2 = Task('Task2', query_definition="SELECT * FROM `table2`", stage=1)
    pipeline.add_task(task1)
    pipeline.add_task(task2)
    with patch.object(pipeline, 'execute_stage') as mock_execute_stage:
        pipeline.execute_stage_by_number(1)
        mock_execute_stage.assert_called_once_with([task1, task2])
