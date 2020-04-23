from typing import Any, List, Dict

from airflow.models.baseoperator import BaseOperator
from airflow.models.xcom import XCOM_RETURN_KEY


class XComArg:
    """
    Class that represents a XCom push from a previous operator. Defaults to "return_value" as only key.
    """
    def __init__(self, operator: BaseOperator, keys: List[str]):
        self._operator = operator
        self._keys = keys or [XCOM_RETURN_KEY]

    def set_upstream(self, t):
        self._operator.set_upstream(t)

    def set_downstream(self, t):
        self._operator.set_downstream(t)

    def __lshift__(self, other):
        self.set_upstream(other)

    def __rshift__(self, other):
        self.set_downstream(other)

    def resolve(self, context: dict) -> Dict[Any]:
        """
        Pull XCom value for the existing arg.
        """
        all_results = {}
        for key in self._keys:
            all_results[key] = self._operator.xcom_pull(
                context=context,
                task_ids=[self._operator.task_id],
                key=key,
                dag_id=self._operator.dag.dag_id)
        return all_results

    # def __getitem__(self, key: str) -> 'XComArg':
    #     """Return an XComArg for the specified key and the same operator as the current one.
    # """
    #     return XComArg(self._operator, [key])
