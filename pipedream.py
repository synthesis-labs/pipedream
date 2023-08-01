import json
from re import A
import commands
import branch_manager
from resource.trigger import trigger_dag
import script_manager
import argparse
import sys
import resource
from resource.create import DeploymentManager
from resource.glue import GlueJobManager
from resource.stepfunctions import StepFunctionManager
from infrastructure.s3 import S3Manager
from infrastructure.glue import deploy_glue_jobs
import boto3
import utils.file_operations
import delta_generator
import s3fs

session = boto3.session.Session()

glue_client = session.client("glue")
sfn_client = session.client("stepfunctions")
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
s3_filesystem = s3fs.S3FileSystem()


glue_job_manager = GlueJobManager(glue_client)
step_function_manager = StepFunctionManager(sfn_client)
resource_factory = DeploymentManager(glue_job_manager, step_function_manager)
script_manager = script_manager.ScriptManager(s3_resource)
s3_manager = S3Manager(s3_client, s3_filesystem)

BRANCH_DATA = "branches/branches.json"
branch_manager = branch_manager.Branch(utils.file_operations, BRANCH_DATA)


def operation_parser(args):
    parser = argparse.ArgumentParser(description="Pipedream Data Pipeline Manager")

    parser.add_argument(
        "operation",
        choices=[
            "plot",
            "nodes",
            "branches",
            "create-branch",
            "delete-branch",
            "deploy-branch",
            "destroy-branch",
            "trigger-branch",
            "generate-main",
            "deltas",
            "import-dag",
        ],
        help="Operation to perform on your pipeline.",
    )

    return parser.parse_args(args)


def create_branch_parser(args):
    parser = argparse.ArgumentParser(
        description="Create a branch with a new name in the pipeline at the specified node"
    )
    parser.add_argument(
        "node", help="The location of your terraform state file in which the instances are described", default=""
    )
    parser.add_argument(
        "name",
        help="The name of the newly created branch. This name will be appended to all your pipeline's created resources and data.",
        default="",
    )
    return parser.parse_args(args)


def trigger_branch_parser(args):
    parser = argparse.ArgumentParser(description="Manually trigger a branch")
    parser.add_argument("name", help="Name of the branch that should be triggered", default="")
    return parser.parse_args(args)


def delete_branch_parser(args):
    parser = argparse.ArgumentParser(description="Delete a previously created branch")
    parser.add_argument("name", help="Name of the branch that should be deleted", default="")
    return parser.parse_args(args)


def deploy_branch_parser(args):
    parser = argparse.ArgumentParser(description="Deploy a created branch")
    parser.add_argument("name", help="Name of the branch that should be deployed", default="")
    return parser.parse_args(args)


def destroy_branch_parser(args):
    parser = argparse.ArgumentParser(description="Destroy a currently deployed branch")
    parser.add_argument("name", help="Name of the branch that should be destroyed", default="")
    return parser.parse_args(args)


def branch_delta_parser(args):
    parser = argparse.ArgumentParser(description="Generate the deltas produced by the branch")
    parser.add_argument("name", help="Name of the branch that should be used to compare deltas", default="")
    return parser.parse_args(args)


def import_dag_parser(args):
    parser = argparse.ArgumentParser(description="Import dag from AWSGlue")
    parser.add_argument(
        "query_string", help="Sting that should be used to match glue jobs to be imported into dag", default=""
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    raw_args = sys.argv

    default = "-h" if len(raw_args) < 2 else raw_args[1]
    args = operation_parser([default])

    if args.operation == "plot":
        commands.plot_main(utils.file_operations)

    elif args.operation == "nodes":
        print(commands.list_nodes(utils.file_operations))

    elif args.operation == "branches":
        print(commands.list_branches(branch_manager))

    elif args.operation == "import-dag":
        args = import_dag_parser(raw_args[2:])
        print(commands.import_dag(glue_job_manager, utils.file_operations, args.query_string))

    elif args.operation == "create-branch":
        args = create_branch_parser(raw_args[2:])
        print(commands.create_branch(utils.file_operations, branch_manager, script_manager, args.node, args.name))

    elif args.operation == "trigger-branch":
        args = trigger_branch_parser(raw_args[2:])
        commands.trigger_branch(branch_manager, trigger_dag, args.name)

    elif args.operation == "delete-branch":
        args = delete_branch_parser(raw_args[2:])
        print(commands.delete_branch(branch_manager, args.name))

    elif args.operation == "deploy-branch":
        args = deploy_branch_parser(raw_args[2:])
        print(
            commands.deploy_branch(utils.file_operations, branch_manager, script_manager, resource_factory, args.name)
        )

    elif args.operation == "destroy-branch":
        args = destroy_branch_parser(raw_args[2:])
        print(
            commands.destroy_branch(utils.file_operations, branch_manager, script_manager, resource_factory, args.name)
        )

    elif args.operation == "generate-main":
        print("Generating main concrete branch")
        commands.generate_main(utils.file_operations, branch_manager)
        print("Main concrete branch generated")

    elif args.operation == "deltas":
        args = branch_delta_parser(raw_args[2:])
        commands.generate_deltas(utils.file_operations, branch_manager, delta_generator, args.name)