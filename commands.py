from prettytable import PrettyTable
from branch_manager import Branch
from resource.create import DeploymentManager
from resource.glue import GlueJobManager
from resource.trigger import trigger_dag
import dag_manager
from script_manager import ScriptManager
from utils import file_operations
import delta_generator

MAIN_ABSTRACT_BRANCH_LOCATION = "graphs/main_abstract.graph.json"
MAIN_BRANCH_NAME = "main"



def list_nodes(file_operations: file_operations):
    graph = file_operations.read_graph(MAIN_ABSTRACT_BRANCH_LOCATION)
    return str(graph.nodes)


def plot_main(file_operations: file_operations):
    graph = file_operations.read_graph(MAIN_ABSTRACT_BRANCH_LOCATION)
    dag_manager.plot_graph(graph)


def list_branches(branch_manager: Branch):
    branches = branch_manager.get_all_branches()

    pretty_branch_table = PrettyTable()
    pretty_branch_table.field_names = ["Name", "Node", "Deployed", "Branch Path", "Script Path"]
    for branch in branches:
        pretty_branch_table.add_row(
            [
                branch["name"],
                branch["branch_node"],
                branch["deployed"],
                branch["concrete_branch"],
                branch["local_script"],
            ]
        )

    return f"\nCurrent branches:\n{pretty_branch_table}"


def create_branch(
    file_operations: file_operations, branch_manager: Branch, script_manager: ScriptManager, node_name, branch_name
):
    graph = file_operations.read_graph(MAIN_ABSTRACT_BRANCH_LOCATION)

    if not node_name in graph.nodes:
        return f"Node {node_name} does not exist. Valid nodes are: \n{graph.nodes}"

    if node_name in ["start", "stop"]:
        return '"start" and "stop" nodes cannot be branched.'

    if branch_manager.branch_exists(branch_name):
        return f"Branch with name {branch_name} already exists"

    # Create a branch folder
    branch_manager.create_branch_folder(branch_name)

    # Create a branched DAG
    branched_concrete_dag = dag_manager.create_concrete_branch(graph, node_name, branch_name)
    branch_location = branch_manager.concrete_path(branch_name)
    file_operations.write_graph(branched_concrete_dag, branch_location)

    # Copy branched script to local directory
    main_concrete_dag_path = branch_manager.concrete_path(MAIN_BRANCH_NAME)
    main_concrete_dag = file_operations.read_graph(main_concrete_dag_path)
    remote_script_location = dag_manager.get_script_location_of_node(main_concrete_dag, node_name)
    local_script_location = branch_manager.local_script_location(node_name, branch_name)
    script_manager.copy_script_to_local(remote_script_location, local_script_location)

    # Save all the branch data
    branch_manager.create_new_branch(branch_name, node_name, branch_location, local_script_location)

    return f'Branch successfully created. Run "deploy-branch {branch_name}" in order to deploy this newly created branch to AWS.'


def delete_branch(branch_manager: Branch, branch_name):
    if branch_name == "main":
        return 'Branch "main" cannot be deleted.'

    if not branch_manager.branch_exists(branch_name):
        return f"Branch with name {branch_name} does not exist."

    if branch_manager.branch_deployed(branch_name):
        return 'Branch is currently deployed and cannot be delete. Run "destroy-branch {branch-name}" to destroy the branch.'

    branch_manager.delete_branch(branch_name)
    return f"Branch with name {branch_name} has been successfully deleted."


def deploy_branch(
    file_operations: file_operations,
    branch_manager: Branch,
    script_manager: ScriptManager,
    resource_factory: DeploymentManager,
    branch_name,
):

    if not branch_manager.branch_exists(branch_name):
        return f"Branch {branch_name} does not exist."

    # get branch details
    branch = branch_manager.get_branch(branch_name)
    concrete_path = branch_manager.concrete_path(branch_name)
    dag = file_operations.read_graph(concrete_path)

    print(f"Deploying branch {branch_name}...")

    if not branch_name == "main":
        dag = resource_factory.deploy(dag)
    else:
        dag = resource_factory._deploy_orch(dag)

    file_operations.write_graph(dag, concrete_path)

    # upload script
    if not branch_name == "main":
        s3_script_location = dag_manager.get_script_location_of_node(dag, branch["branch_node"])
        script_manager.upload_script(branch["local_script"], s3_script_location)

    # Mark as deployed
    branch_manager.deploy_branch(branch_name)

    return f"Branch {branch_name} deployed."


def destroy_branch(
    file_operations: file_operations,
    branch_manager: Branch,
    script_manager: ScriptManager,
    resource_factory: DeploymentManager,
    branch_name,
):
    if branch_name == "main":
        return 'Branch "main" cannot be destroyed.'

    if not branch_manager.branch_exists(branch_name):
        return f"Branch {branch_name} does not exist."

    if not branch_manager.branch_deployed(branch_name):
        return f"Branch {branch_name} is not currently deployed and cannot be destroyed."

    # get branch details
    branch = branch_manager.get_branch(branch_name)
    dag = file_operations.read_graph(branch["concrete_branch"])

    print(f"Destroying branch {branch_name}...")
    dag = resource_factory.destroy(dag)
    file_operations.write_graph(dag, branch["concrete_branch"])

    # delete script
    s3_script_location = dag_manager.get_script_location_of_node(dag, branch["branch_node"])
    script_manager.delete_script(s3_script_location)

    # delete data
    s3_data_locations = dag_manager.get_branch_data_paths(dag, branch["branch_node"])
    print(s3_data_locations)
    for location in s3_data_locations:
        script_manager.delete_path(location)

    # Mark as not deployed
    branch_manager.destroy_branch(branch_name)

    return f"Branch {branch_name} destroyed."


def trigger_branch(file_operations: file_operations, branch_manager: Branch, trigger_dag: trigger_dag, branch_name):

    if not branch_manager.branch_exists(branch_name):
        return f"Branch with name {branch_name} does not exist"

    if not branch_manager.branch_deployed(branch_name):
        return f"Branch with name {branch_name} is not deployed"

    print(f"Triggering branch: {branch_name}")
    concrete_path = branch_manager.concrete_path(branch_name)
    dag = file_operations.read_graph(concrete_path)
    trigger_dag(dag)


def generate_deltas(
    file_operations: file_operations, branch_manager: Branch, delta_generator: delta_generator, branch_name
):

    if not branch_manager.branch_exists(branch_name):
        return f"Branch {branch_name} does not exist."

    if not branch_manager.branch_deployed(branch_name):
        return f"Branch {branch_name} is not currently deployed."

    branch = branch_manager.get_branch(branch_name)
    dag = file_operations.read_graph(branch["concrete_branch"])
    main_dag_path = branch_manager.concrete_path(MAIN_BRANCH_NAME)
    main_dag = file_operations.read_graph(main_dag_path)

    main_data_paths = dag_manager.get_branch_data_paths(main_dag, branch["branch_node"])
    branch_data_paths = dag_manager.get_branch_data_paths(dag, branch["branch_node"])

    for main_path, branch_path in zip(main_data_paths, branch_data_paths):
        print(f"pulling data for : {main_path}")
        df_main = delta_generator.s3_parquet_to_pd(main_path)

        print(f"pulling data for : {branch_path}")
        df_branch = delta_generator.s3_parquet_to_pd(branch_path)

        primary_key = df_main.columns[0]
        df = delta_generator.calculate_delta(df_main, df_branch, primary_key)
        print("Data changes:")
        print(df)


def generate_main(file_operations: file_operations, branch_manager: Branch):
    graph = file_operations.read_graph(MAIN_ABSTRACT_BRANCH_LOCATION)
    main_concrete = dag_manager.create_concrete(graph, True)
    main_concrete.graph["metadata"]["branch"] = "main"
    main_concrete_path = branch_manager.concrete_path(MAIN_BRANCH_NAME)
    file_operations.write_graph(main_concrete, main_concrete_path)
    return main_concrete


def import_dag(glue_job_manager: GlueJobManager, file_operations: file_operations, query_string):
    print("Constructing job graph....")
    filtered_metadata = glue_job_manager.get_jobs(query_string)
    print(filtered_metadata)

    G = dag_manager.create_graph_from_glue_jobs(filtered_metadata)
    file_operations.write_graph(G, "graphs/jobs.json")
    print("Created file to graphs/jobs.json")