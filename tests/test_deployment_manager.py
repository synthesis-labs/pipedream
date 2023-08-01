import unittest
from resource.create import DeploymentManager
from resource.glue import GlueJobManager
from unittest.mock import Mock
import utils.file_operations
from pathlib import Path

test_graph_json_path = Path(__file__).parent / "test_graph.json"


class WhenCallingDeployNodesGivenAValidGraphThenTest(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        graph.nodes["extract"]["metadata"]["deploy"] = True
        self.mock_glue_job_manager = Mock(spec=GlueJobManager)
        self.mock_glue_job_manager.create_glue_job.return_value = None
        deployment_manager = DeploymentManager(self.mock_glue_job_manager, None)
        self.result = deployment_manager._deploy_nodes(graph)

    def test_that_the_create_glue_job_call_is_made_once(self):
        self.mock_glue_job_manager.create_glue_job.assert_called_once()

