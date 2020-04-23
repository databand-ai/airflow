from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'dbnd',
    'depends_on_past': True,
    'start_date': datetime(2020, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

value_1 = 8

def return_value_1():
    return value_1


def assert_is_value_1(num: int):
    assert num == value_1


class TestXComArg(object):
    def test_xcom_pass_to_op(self):
        with DAG(dag_id="test_xcom_pass_to_op", default_args=default_args, schedule_interval="* * * * *") as dag:
            operator = PythonOperator(python_callable=return_value_1, task_id="return_value_1", do_xcom_push=True)
            xarg = XComArg(operator)
            operator2 = PythonOperator(python_callable=assert_is_value_1, op_args=[xarg], task_id="assert_is_8")
            operator >> operator2
        operator.run(ignore_ti_state=True, ignore_first_depends_on_past=True)
        operator2.run(ignore_ti_state=True, ignore_first_depends_on_past=True)
