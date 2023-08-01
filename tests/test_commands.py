import pandas as pd
import unittest
from commands import (
    create_branch,
    delete_branch,
    deploy_branch,
    destroy_branch,
    generate_deltas,
    generate_main,
    import_dag,
    list_branches,
    list_nodes,
    trigger_branch,
)
from unittest.mock import Mock
from branch_manager import Branch
import script_manager
from resource.create import DeploymentManager
import utils.file_operations
from resource.glue import GlueJobManager
import delta_generator
from pathlib import Path

test_graph_json_path = Path(__file__).parent / "test_graph.json"


class WhenCreatingAValidBranch(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)

        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = False
        mock_branch_manager.create_new_branch.return_value = None

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph
        mock_file_operations.write_graph.return_value = graph

        mock_script_manager = Mock(spec=script_manager.ScriptManager)
        mock_script_manager.copy_script_to_local.return_value = None

        self.results = create_branch(
            mock_file_operations, mock_branch_manager, mock_script_manager, node_name="transform", branch_name="test"
        )

    def test_branch_is_successfully_created(self):

        assert (
            self.results
            == 'Branch successfully created. Run "deploy-branch test" in order to deploy this newly created branch to AWS.'
        )

class WhenCreatingABranchGivenTheNodeDoesNotExist(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)

        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = False
        mock_branch_manager.create_new_branch.return_value = None

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph
        mock_file_operations.write_graph.return_value = graph

        mock_script_manager = Mock(spec=script_manager.ScriptManager)
        mock_script_manager.copy_script_to_local.return_value = None

        self.results = create_branch(
            mock_file_operations, mock_branch_manager, mock_script_manager, node_name="nodeDoesNotExist", branch_name="test"
        )

    def test_branch_is_not_created_due_to_node_not_existing(self):
        assert (
            self.results
            == "Node nodeDoesNotExist does not exist. Valid nodes are: \n['start', 'extract', 'transform', 'load', 'stop']"
        )

class WhenCreatingABranchGivenTheNodeStartOrStop(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)

        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = False
        mock_branch_manager.create_new_branch.return_value = None

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph
        mock_file_operations.write_graph.return_value = graph

        mock_script_manager = Mock(spec=script_manager.ScriptManager)
        mock_script_manager.copy_script_to_local.return_value = None

        self.results = create_branch(
            mock_file_operations, mock_branch_manager, mock_script_manager, node_name="stop", branch_name="test"
        )

    def test_branch_is_not_created_due_to_node_being_start_or_stop(self):
        assert (
            self.results
            == '"start" and "stop" nodes cannot be branched.'
        )



class WhenDeletingAValidBranch(unittest.TestCase):
    def setUp(self):
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = False
        mock_branch_manager.delete_branch.return_value = None

        self.results = delete_branch(mock_branch_manager, branch_name="test")

    def test_branch_is_successfully_deleted(self):
        assert self.results == "Branch with name test has been successfully deleted."

class WhenDeletingTheMainBranch(unittest.TestCase):
    def setUp(self):
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = False
        mock_branch_manager.delete_branch.return_value = None

        self.results = delete_branch(mock_branch_manager, branch_name="main")

    def test_branch_is_not_deleted_due_to_main(self):
        assert self.results == 'Branch "main" cannot be deleted.'

class WhenDeletingABranchThatDoesNotExist(unittest.TestCase):
    def setUp(self):
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = False
        mock_branch_manager.branch_deployed.return_value = False
        mock_branch_manager.delete_branch.return_value = None

        self.results = delete_branch(mock_branch_manager, branch_name="test")

    def test_branch_is_not_deleted_due_to_not_existing(self):
        assert self.results == 'Branch with name test does not exist.'

class WhenDeletingABranchThatIsDeployed(unittest.TestCase):
    def setUp(self):
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = True
        mock_branch_manager.delete_branch.return_value = None

        self.results = delete_branch(mock_branch_manager, branch_name="test")

    def test_branch_is_not_deleted_due_to_deployment(self):
        assert self.results == 'Branch is currently deployed and cannot be delete. Run "destroy-branch {branch-name}" to destroy the branch.'


class WhenDeployingAValidBranch(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = False
        mock_branch_manager.get_branch.return_value = {
            "name": "test",
            "branch_node": "transform",
            "concrete_branch": "branches/main/main_concrete.graph.json",
            "local_script": "NA",
            "deployed": True,
        }
        mock_branch_manager.deploy_branch.return_value = ""

        mock_script_manager = Mock(spec=script_manager.ScriptManager)
        mock_script_manager.upload_script.return_value = None

        mock_resource_factory = Mock(spec=DeploymentManager)
        mock_resource_factory.deploy.return_value = graph

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph
        mock_file_operations.write_graph.return_value = graph

        self.results = deploy_branch(
            mock_file_operations, mock_branch_manager, mock_script_manager, mock_resource_factory, branch_name="test"
        )

    def test_branch_is_successfully_deployed(self):
        assert self.results == "Branch test deployed."

class WhenDeployingABranchThatDoesNotExist(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = False
        mock_branch_manager.branch_deployed.return_value = False
        mock_branch_manager.get_branch.return_value = {
            "name": "test",
            "branch_node": "transform",
            "concrete_branch": "branches/main/main_concrete.graph.json",
            "local_script": "NA",
            "deployed": True,
        }
        mock_branch_manager.deploy_branch.return_value = ""

        mock_script_manager = Mock(spec=script_manager.ScriptManager)
        mock_script_manager.upload_script.return_value = None

        mock_resource_factory = Mock(spec=DeploymentManager)
        mock_resource_factory.deploy.return_value = graph

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph
        mock_file_operations.write_graph.return_value = graph

        self.results = deploy_branch(
            mock_file_operations, mock_branch_manager, mock_script_manager, mock_resource_factory, branch_name="test"
        )

    def test_branch_is_not_deployed_due_to_not_existing(self):
        assert self.results == "Branch test does not exist."

class WhenDestroyingAValidBranch(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)

        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.get_branch.return_value = {
            "name": "test",
            "branch_node": "transform",
            "concrete_branch": "branches/main/main_concrete.graph.json",
            "local_script": "NA",
            "deployed": True,
        }
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = True
        mock_branch_manager.delete_branch.return_value = None

        mock_resource_factory = Mock(spec=DeploymentManager)
        mock_resource_factory.destroy.return_value = graph

        mock_script_manager = Mock(spec=script_manager.ScriptManager)
        mock_script_manager.delete_script.return_value = None
        mock_script_manager.delete_path.return_value = None

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        self.results = destroy_branch(
            mock_file_operations, mock_branch_manager, mock_script_manager, mock_resource_factory, branch_name="test"
        )

    def test_branch_is_successfully_destroyed(self):
        assert self.results == "Branch test destroyed."

class WhenDestroyingABranchThatDoesNotExist(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)

        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.get_branch.return_value = {
            "name": "test",
            "branch_node": "transform",
            "concrete_branch": "branches/main/main_concrete.graph.json",
            "local_script": "NA",
            "deployed": True,
        }
        mock_branch_manager.branch_exists.return_value = False
        mock_branch_manager.branch_deployed.return_value = True
        mock_branch_manager.delete_branch.return_value = None

        mock_resource_factory = Mock(spec=DeploymentManager)
        mock_resource_factory.destroy.return_value = graph

        mock_script_manager = Mock(spec=script_manager.ScriptManager)
        mock_script_manager.delete_script.return_value = None
        mock_script_manager.delete_path.return_value = None

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        self.results = destroy_branch(
            mock_file_operations, mock_branch_manager, mock_script_manager, mock_resource_factory, branch_name="test"
        )

    def test_branch_is_not_destroyed_due_to_not_existing(self):
        assert self.results == "Branch test does not exist."

class WhenDestroyingABranchThatIsNotDeployed(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)

        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.get_branch.return_value = {
            "name": "test",
            "branch_node": "transform",
            "concrete_branch": "branches/main/main_concrete.graph.json",
            "local_script": "NA",
            "deployed": True,
        }
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = False
        mock_branch_manager.delete_branch.return_value = None

        mock_resource_factory = Mock(spec=DeploymentManager)
        mock_resource_factory.destroy.return_value = graph

        mock_script_manager = Mock(spec=script_manager.ScriptManager)
        mock_script_manager.delete_script.return_value = None
        mock_script_manager.delete_path.return_value = None

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        self.results = destroy_branch(
            mock_file_operations, mock_branch_manager, mock_script_manager, mock_resource_factory, branch_name="test"
        )

    def test_branch_is_not_destroyed_due_to_not_being_deployed(self):
        assert self.results == "Branch test is not currently deployed and cannot be destroyed."

class WhenDestroyingTheMainBranch(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)

        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.get_branch.return_value = {
            "name": "test",
            "branch_node": "transform",
            "concrete_branch": "branches/main/main_concrete.graph.json",
            "local_script": "NA",
            "deployed": True,
        }
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = True
        mock_branch_manager.delete_branch.return_value = None

        mock_resource_factory = Mock(spec=DeploymentManager)
        mock_resource_factory.destroy.return_value = graph

        mock_script_manager = Mock(spec=script_manager.ScriptManager)
        mock_script_manager.delete_script.return_value = None
        mock_script_manager.delete_path.return_value = None

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        self.results = destroy_branch(
            mock_file_operations, mock_branch_manager, mock_script_manager, mock_resource_factory, branch_name="main"
        )

    def test_branch_is_not_destroyed_due_to_being_main(self):
        assert self.results == 'Branch "main" cannot be destroyed.'

class WhenListingTheBranches(unittest.TestCase):
    def setUp(self):
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.get_all_branches.return_value = [
            {
                "name": "test",
                "branch_node": "transform",
                "concrete_branch": "branches/main/main_concrete.graph.json",
                "local_script": "NA",
                "deployed": True,
            }
        ]

        self.results = list_branches(mock_branch_manager)

    def test_branch_listing(self):
        assert self.results.startswith("\nCurrent")


class WhenListingTheMainNodes(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        self.results = list_nodes(mock_file_operations)

    def test_list_main_nodes(self):
        assert self.results == "['start', 'extract', 'transform', 'load', 'stop']"


class WhenTriggeringAValidBranch(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = True

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        mock_trigger = lambda m: None
        self.results = trigger_branch(mock_file_operations, mock_branch_manager, mock_trigger, "test")

    def test_trigger_branch(self):
        assert self.results is None


class WhenTriggeredBranchDoesNotExist(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = False
        mock_branch_manager.branch_deployed.return_value = True

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        mock_trigger = lambda m: None
        self.results = trigger_branch(mock_file_operations, mock_branch_manager, mock_trigger, "test")

    def test_trigger_branch_that_does_not_exist(self):
        assert self.results.endswith("exist")

class WhenTriggeredBranchIsNotDeployed(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = False

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        mock_trigger = lambda m: None
        self.results = trigger_branch(mock_file_operations, mock_branch_manager, mock_trigger, "test")

    def test_trigger_branch_that_is_not_deployed(self):
        assert self.results.endswith("deployed")

class WhenGeneratingTheMainBranch(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = True

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        self.results = generate_main(mock_file_operations, mock_branch_manager)

    def test_generate_main_concrete_branch(self):
        assert self.results is not None

    def test_branch_name_is_specified(self):
        assert self.results.graph["metadata"]["branch"] == "main"


class WhenImportingDag(unittest.TestCase):
    def setUp(self):
        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.write_graph.return_value = None

        mock_glue_jobs = Mock(spec=GlueJobManager)
        mock_glue_jobs.get_jobs.return_value = []

        self.results = import_dag(mock_glue_jobs, mock_file_operations, "query_string")

    def test_import_dag(self):
        assert self.results is None


class WhenGeneratingDeltas(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = True
        mock_branch_manager.get_branch.return_value = {
            "name": "test",
            "branch_node": "transform",
            "concrete_branch": "branches/main/main_concrete.graph.json",
            "local_script": "NA",
            "deployed": True,
        }

        df = pd.DataFrame(
            {
                "host_name": ["t", "Jaguar", "MG", "MINI", "Rover", "test"],
                "registration": ["t", "1234", "BC99BCDF", "CD00CDE", "DE01DEF", "DE01DEFa"],
                "registration2": ["t", "1234", "BC99BCDFa", "CD00CDE", "DE01DEF", "DE01DEF"],
            }
        )

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        mock_delta_generator = Mock(spec=delta_generator)
        mock_delta_generator.s3_parquet_to_pd.return_value = df
        mock_delta_generator.calculate_delta.return_value = df

        self.results = generate_deltas(mock_file_operations, mock_branch_manager, mock_delta_generator, "branch_name")

    def test_generate_deltas(self):
        assert self.results is None

class WhenGeneratingDeltasAndBranchDoesNotExist(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = False
        mock_branch_manager.branch_deployed.return_value = True
        mock_branch_manager.get_branch.return_value = {
            "name": "test",
            "branch_node": "transform",
            "concrete_branch": "branches/main/main_concrete.graph.json",
            "local_script": "NA",
            "deployed": True,
        }

        df = pd.DataFrame(
            {
                "host_name": ["t", "Jaguar", "MG", "MINI", "Rover", "test"],
                "registration": ["t", "1234", "BC99BCDF", "CD00CDE", "DE01DEF", "DE01DEFa"],
                "registration2": ["t", "1234", "BC99BCDFa", "CD00CDE", "DE01DEF", "DE01DEF"],
            }
        )

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        mock_delta_generator = Mock(spec=delta_generator)
        mock_delta_generator.s3_parquet_to_pd.return_value = df
        mock_delta_generator.calculate_delta.return_value = df

        self.results = generate_deltas(mock_file_operations, mock_branch_manager, mock_delta_generator, "branch_name")

    def test_generate_deltas_fail_due_to_branch_not_existing(self):
        assert self.results.endswith("exist.")      

class WhenGeneratingDeltasAndBranchIsNotDeployed(unittest.TestCase):
    def setUp(self):
        graph = utils.file_operations.read_graph(test_graph_json_path)
        mock_branch_manager = Mock(spec=Branch)
        mock_branch_manager.branch_exists.return_value = True
        mock_branch_manager.branch_deployed.return_value = False
        mock_branch_manager.get_branch.return_value = {
            "name": "test",
            "branch_node": "transform",
            "concrete_branch": "branches/main/main_concrete.graph.json",
            "local_script": "NA",
            "deployed": True,
        }

        df = pd.DataFrame(
            {
                "host_name": ["t", "Jaguar", "MG", "MINI", "Rover", "test"],
                "registration": ["t", "1234", "BC99BCDF", "CD00CDE", "DE01DEF", "DE01DEFa"],
                "registration2": ["t", "1234", "BC99BCDFa", "CD00CDE", "DE01DEF", "DE01DEF"],
            }
        )

        mock_file_operations = Mock(spec=utils.file_operations)
        mock_file_operations.read_graph.return_value = graph

        mock_delta_generator = Mock(spec=delta_generator)
        mock_delta_generator.s3_parquet_to_pd.return_value = df
        mock_delta_generator.calculate_delta.return_value = df

        self.results = generate_deltas(mock_file_operations, mock_branch_manager, mock_delta_generator, "branch_name")

    def test_generate_deltas_fail_due_to_branch_not_deployed(self):
        assert self.results.endswith("deployed.") 

if __name__ == "__main__":
    unittest.main()
