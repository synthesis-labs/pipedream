from utils import file_operations

class Branch:
    def __init__(self, file_operations:file_operations, location):
        self.location = location
        self.file_operations = file_operations

    def get_all_branches(self):
        branches = self.file_operations.read_json_file(self.location)
        return branches

    def get_branch(self, branch_name):
        branches = self.get_all_branches()
        return [b for b in branches if b["name"] == branch_name][0]

    def save_branch_data(self, branch_data):
        self.file_operations.write_json_file(self.location, branch_data)

    def create_branch_folder(self, branch_name):
        path = self.get_branch_path(branch_name)
        self.file_operations.create_folder(path)
        return path

    def get_all_branch_names(self):
        all_branch_names = [branch["name"] for branch in self.get_all_branches()]
        return all_branch_names

    def branch_deployed(self,branch_name):
        branches = self.get_all_branches()
        return branch_name in [b["name"] for b in branches if b["deployed"]]

    def branch_exists(self, branch_name):
        return branch_name in self.get_all_branch_names()

    def get_branch_path(self, branch_name):
        return f"branches/{branch_name}"

    def local_script_location(self, node_name, branch_name):
        return f"{self.get_branch_path(branch_name)}/{node_name}_{branch_name}_script.py"

    def concrete_path(self, branch_name):
        return f"{self.get_branch_path(branch_name)}/{branch_name}_concrete.graph.json"


    def create_new_branch(self, branch_name, branch_node, graph_location, script_location):
        data = self.get_all_branches()
        data.append(
            {
                "name": branch_name,
                "branch_node": branch_node,
                "concrete_branch": graph_location,
                "local_script": script_location,
                "deployed": False,
            }
        )
        self.save_branch_data(data)

    def deploy_branch(self, branch_name):
        branches = self.get_all_branches()
        branch = [b for b in branches if b["name"] == branch_name][0]
        branch["deployed"] = True
        self.save_branch_data(branches)

    def destroy_branch(self, branch_name):
        branches = self.get_all_branches()
        branch = [b for b in branches if b["name"] == branch_name][0]
        branch["deployed"] = False
        self.save_branch_data(branches)

    def delete_branch(self, branch_name):
        data = self.get_all_branches()
        data = [b for b in data if not b["name"] == branch_name]
        self.save_branch_data(data)

        self.file_operations.delete_folder(self.get_branch_path(branch_name))

        print(f'Branch "{branch_name}" successfully destroyed.')
        return data



    



  