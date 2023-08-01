from networkx import DiGraph
from . import adaptors

FUNCTION_MAP = {
    "aws_stepfunctions": adaptors.aws_stepfunctions
}
#TODO: Refactor
def trigger_dag(dag: DiGraph):
    dag_orch = dag.graph["metadata"]["orchestration"]
    FUNCTION_MAP[dag_orch](dag)
    