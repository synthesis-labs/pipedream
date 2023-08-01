from pathlib import Path
from unittest.mock import Mock
from resource.stepfunctions import StepFunctionManager
import unittest
from utils import file_operations
import json

test_graph_json_path = Path(__file__).parent / "test_graph.json"

class WhenCallingBuildASLGivenAValidGraphThenTest(unittest.TestCase):
    def setUp(self):
        graph = file_operations.read_graph(test_graph_json_path)
        step_function_manager = StepFunctionManager(None)
        self.results = json.loads(step_function_manager.build_asl(graph))

    def test_that_the_asl_should_start_at_first_node(self):
        assert self.results["StartAt"] == "extract"
    
    def test_that_the_asl_should_contain_the_correct_steps(self):
        assert len(self.results["States"]) == 3
