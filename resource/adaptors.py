import boto3
from networkx import DiGraph
from .stepfunctions import StepFunctionManager

#TODO: Refactor
def aws_stepfunctions(dag: DiGraph):
    sfn_client = StepFunctionManager(sfn_client=boto3.client("stepfunctions"))
    sfn_arn = dag.graph["metadata"]["state"]["arn"]
    return sfn_client.trigger_step_function(sfn_arn=sfn_arn)

