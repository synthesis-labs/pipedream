import unittest
import dag_manager
from utils import file_operations
from pathlib import Path

test_graph_json_path = Path(__file__).parent / "test_graph.json"


class TestGetDataPaths(unittest.TestCase):
    def setUp(self):
        self.G = file_operations.read_graph(test_graph_json_path)
        self.results = dag_manager.get_branch_data_paths(self.G, "extract")

    def test_results_should_be_correct(self):
        self.assertEqual(
            self.results,
            [
                "s3://hb-machine-learning-bucket/pipedream/{branch-name}/data/bronze/",
                "s3://hb-machine-learning-bucket/pipedream/{branch-name}/data/silver/",
                "s3://hb-machine-learning-bucket/pipedream/{branch-name}/data/gold/",
            ],
        )


class TestGraphTrimming(unittest.TestCase):
    def setUp(self):
        self.G = file_operations.read_graph(test_graph_json_path)
        self.results = dag_manager.create_concrete_branch(self.G, "transform", "test")

    def test_number_of_nodes_should_be_correct(self):
        self.assertEqual(len(self.results.nodes), 4)

    def test_nodes_should_be_correct(self):
        self.assertEqual(list(self.results.nodes), ["start", "transform", "load", "stop"])

    def test_edges_should_be_correct(self):
        self.assertEqual(list(self.results.edges("start", "transform")), [("start", "transform", None)])

    def test_input_paths_should_be_correct_for_first_node(self):
        self.assertEqual(
            self.results.nodes["transform"]["metadata"]["properties"]["DefaultArguments"]["--BRONZE_DATA_SOURCE"],
            "s3://hb-machine-learning-bucket/pipedream/main/data/bronze/",
        )

    def test_input_paths_should_be_correct_for_subsequent_nodes(self):
        self.assertEqual(
            self.results.nodes["load"]["metadata"]["properties"]["DefaultArguments"]["--SILVER_DATA_SOURCE"],
            "s3://hb-machine-learning-bucket/pipedream/test/data/silver/",
        )

    def test_output_paths_should_be_correct(self):
        self.assertEqual(
            self.results.nodes["transform"]["metadata"]["output_data"],
            {"silver": "s3://hb-machine-learning-bucket/pipedream/test/data/silver/"},
        )

    def test_output_paths_should_be_correct_for_subsequent_nodes(self):
        self.assertEqual(
            self.results.nodes["load"]["metadata"]["properties"]["DefaultArguments"]["--GOLD_DATA_SINK"],
            "s3://hb-machine-learning-bucket/pipedream/test/data/gold/",
        )

    def test_job_name_should_be_correct(self):
        self.assertEqual(
            self.results.nodes["transform"]["metadata"]["properties"]["Name"],
            "pipedream-transform-test",
        )

    def test_script_should_be_correct(self):
        self.assertEqual(
            self.results.nodes["transform"]["metadata"]["properties"]["Command"]["ScriptLocation"],
            "s3://hb-machine-learning-bucket/pipedream/test/scripts/pipedream-transform-main.py",
        )


class TestGraphBranchingExtract(unittest.TestCase):
    def setUp(self):
        self.G = file_operations.read_graph(test_graph_json_path)
        self.results = dag_manager.create_concrete_branch(self.G, "extract", "test")

    def test_number_of_nodes_should_be_correct(self):
        self.assertEqual(len(self.results.nodes), 5)

    def test_nodes_should_be_correct(self):
        self.assertEqual(list(self.results.nodes), ["start", "extract", "transform", "load", "stop"])

    def test_edges_should_be_correct(self):
        self.assertEqual(list(self.results.edges("start", "extract")), [("start", "extract", None)])

    def test_input_paths_should_be_correct(self):
        self.assertEqual(
            self.results.nodes["extract"]["metadata"]["properties"]["job_params"],
            {
                "--SOURCE_S3_PATH": "s3://hb-machine-learning-bucket/data-migration/data/category/ExtractDate=2022-05-20/"
            },
        )

    def test_output_paths_should_be_correct(self):
        self.assertEqual(
            self.results.nodes["extract"]["metadata"]["output_data"],
            {"bronze": "s3://hb-machine-learning-bucket/pipedream/test/data/bronze/"},
        )

class TestGraphBranchingExtract(unittest.TestCase):
    def setUp(self):
        self.G = file_operations.read_graph(test_graph_json_path)
        self.results = dag_manager.get_script_location_of_node(self.G, "extract")

    def test_script_location_should_be_correct(self):
        assert self.results == "s3://hb-machine-learning-bucket/pipedream/{branch-name}/scripts/pipedream-extract-main.py"

    

if __name__ == "__main__":
    unittest.main()
