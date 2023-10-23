from .glue import GlueJobManager
from .stepfunctions import StepFunctionManager
from networkx import DiGraph

DEPLOYMENT_FLAG = "deploy"


class DeploymentManager:
    def __init__(self, glue_job_manager: GlueJobManager, sfn_manager: StepFunctionManager):
        self.glue_job_manager = glue_job_manager
        self.sfn_manager = sfn_manager

    def deploy(self, graph: DiGraph) -> DiGraph:
        self._deploy_nodes(graph)
        self._deploy_orch(graph)
        return graph

    def destroy(self, graph: DiGraph) -> DiGraph:
        self._destroy_nodes(graph)
        self._destroy_orch(graph)
        return graph

    def _deploy_nodes(self, graph):
        glue_nodes = filter(
            lambda x: graph.nodes[x].get("metadata", {}).get("operation", {}) == "glue"
            and graph.nodes[x].get("metadata", {}).get(DEPLOYMENT_FLAG) is True,
            graph.nodes,
        )
        for node in glue_nodes:
            properties = graph.nodes[node]["metadata"]["properties"]
            self.glue_job_manager.create_glue_job(properties)
        return graph

    def _destroy_nodes(self, graph):
        resources = filter(
            lambda x: graph.nodes[x].get("metadata", {}).get("operation", {}) == "glue"
            and graph.nodes[x].get("metadata", {}).get(DEPLOYMENT_FLAG) is True,
            graph.nodes,
        )

        for r in resources:
            data = graph.nodes[r]["metadata"]["properties"]
            self.glue_job_manager.delete_glue_job(job_name=data["Name"])

    def _deploy_orch(self, graph):
        role_arn = graph.graph["metadata"]["role_arn"]
        pipeline_name = f'{graph.graph["metadata"]["pipeline_name"]}-{graph.graph["metadata"]["branch"]}'
        arn = self.sfn_manager.create_step_function(
            sfn_name=f"pypedream-{pipeline_name}",
            role_arn=role_arn,
            definition=self.sfn_manager.build_asl(graph=graph),
        )
        graph.graph["metadata"]["state"]["arn"] = arn
        return graph

    def _destroy_orch(self, graph):
        arn = graph.graph["metadata"]["state"]["arn"]
        self.sfn_manager.delete_step_function(sfn_arn=arn)
        graph.graph["metadata"]["state"]["arn"] = None

