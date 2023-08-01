import json
import os
import shutil
import dag_manager


def read_json_file(path):
    with open(path, "r") as main_file:
        json_string = main_file.read()
        json_object = json.loads(json_string)
        return json_object


def write_json_file(path, data):
    with open(path, "w") as file:
        json_string = json.dumps(data, default=str)
        file.write(json_string)


def create_folder(path):
    os.makedirs(os.path.dirname(path + "/"), exist_ok=True)


def delete_folder(path):
    shutil.rmtree(path)


def read_graph(location):
    json_object = read_json_file(location)
    return dag_manager.from_json(json_object)


def write_graph(G, location):
    json_object = dag_manager.to_json(G)
    write_json_file(location, json_object)

