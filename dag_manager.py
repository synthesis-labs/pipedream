import networkx as nx
from networkx.readwrite import json_graph
import matplotlib.pyplot as plt


def to_json(G):
    return json_graph.node_link_data(G)


def from_json(json_object):
    return json_graph.node_link_graph(json_object)


def plot_graph(G):
    options = {
        # 'font_size': 36,
        # 'node_size': 3000,
        "node_color": "white",
        # 'edgecolors': 'black',
        # 'linewidths': 5,
        # 'width': 5,
    }
    # nx.draw_networkx(G,pos=nx.planar_layout(G), **options)
    nodes = nx.draw_networkx_nodes(G, pos=nx.planar_layout(G), **options)
    labels = nx.draw_networkx_labels(G, pos=nx.planar_layout(G))
    edges = nx.draw_networkx_edges(G, pos=nx.planar_layout(G), arrowstyle="->", arrowsize=10, width=2,)
    ax = plt.gca()
    ax.margins(0.20)
    plt.axis("off")
    plt.show()


def create_concrete_node(branch_name, node, deploy=None):
    # Change node branched node output path details as well as script location details
    if node["metadata"]["properties"].get("Name"):
        node["metadata"]["properties"]["Name"] = node["metadata"]["properties"]["Name"].replace("{branch-name}", branch_name)

    if deploy:
        node["metadata"]["deploy"] = deploy

    if node["metadata"]["properties"]["Command"].get("ScriptLocation"):
        node["metadata"]["properties"]["Command"]["ScriptLocation"] = node["metadata"]["properties"]["Command"]["ScriptLocation"].replace("{branch-name}", branch_name)

    output_data = node["metadata"].get("output_data", {})
    for k, v in output_data.items():
        node["metadata"]["output_data"][k] = v.replace("{branch-name}", branch_name)
        node["metadata"]["properties"]['DefaultArguments'][k] = v.replace("{branch-name}", branch_name)

    return node


def create_branch(H, node_name, branch_name):
    G = nx.DiGraph(H)
    node = G.nodes[node_name]
    node = create_concrete_node(branch_name, node, True)
    if node.get("metadata", False):
        node["metadata"]["include"] = True

    # Change downstream node metadata to use new branch paths for output data
    def change_downstream(node1):
        for neighbor in G.successors(node1):
            neighbor_node = G.nodes[neighbor]
            output_data = neighbor_node["metadata"].get("output_data", {})
            for k, v in output_data.items():
                neighbor_node["metadata"]["output_data"][k] = v.replace("{branch-name}", branch_name)

            # TODO: Check if there is a data dependency
            if neighbor_node.get("metadata", False):
                neighbor_node["metadata"]["include"] = True

            change_downstream(neighbor)

    change_downstream(node_name)
    return G


def trim(H, branch_node_name):
    G = nx.DiGraph(H)
    
    #Add data paths to DefaultArguments
    # TODO: Add more tests that validate that this works for complex graphs
    for node_name in [n for n in G.nodes if n not in ["start", "stop"]]:
        for pred in G.predecessors( node_name ):
            prev_meta = G.nodes[pred].get("metadata",{})
            node_meta = G.nodes[node_name].get("metadata", {})
            edge_meta = G.edges[pred,node_name].get("metadata", {})
            update_dict = {f"--{key}_data_source".upper(): prev_meta.get("output_data", {}).get(key) for key in edge_meta.get("data", []) if key}
            node_meta.get("properties",{}).get("DefaultArguments", {}).update(update_dict)
            update_dict = {f"--{key}_data_sink".upper(): value for key, value in node_meta.get("output_data", {}).items()}
            node_meta.get("properties",{}).get("DefaultArguments", {}).update(update_dict)


    for node_name in [n for n in G.nodes if n not in ["start", "stop"]]:
        node = G.nodes[node_name]
        include = node["metadata"].get("include", False)
        if not include:
            G.remove_node(node_name)
    G.add_edge("start", branch_node_name)

    # Fix broken input data
    node = H.nodes[branch_node_name]

    for pred in H.predecessors(branch_node_name):
        edge = H.get_edge_data(pred, branch_node_name)
        input_data_names = edge["metadata"]["data"]

        if not input_data_names:
            continue

        input_data_paths = H.nodes[pred]["metadata"]["output_data"]
        for data_name in input_data_names:
            # print(G.nodes[branch_node_name])
            G.nodes[branch_node_name]["metadata"]["properties"]["DefaultArguments"][
                f"--{data_name}_data_source".upper()
            ] = input_data_paths[data_name]


    return G


def create_concrete_branch(G, node, branch_name):
    G.graph["metadata"]["branch"] = branch_name
    branched_abstract_graph = nx.DiGraph(create_branch(G, node, branch_name))
    branched_concrete_graph = create_concrete(branched_abstract_graph, False)
    trimmed_branched_concrete_graph = trim(branched_concrete_graph, node)
    return trimmed_branched_concrete_graph


def create_concrete_dag(H, branch_name, deploy=None):
    G = nx.DiGraph(H)
    for node_name in [n for n in G.nodes if n not in ["start", "stop"]]:
        node = G.nodes[node_name]
        node = create_concrete_node(branch_name, node, deploy)

    return G


def create_concrete(G, deploy):
    main_branch_name = "main"
    main_concrete_graph = create_concrete_dag(G, main_branch_name, deploy)
    return main_concrete_graph


def get_script_location_of_node(G, node_name):
    node = G.nodes[node_name]
    # if node['metadata'].get('script_location'):
    return node["metadata"]["properties"]["Command"]["ScriptLocation"]


def get_branch_data_paths(G, node_name):
    def get_down_stream_paths(node):
        n = G.nodes[node]
        output_data = n["metadata"].get("output_data", {})
        paths = [v for k, v in output_data.items()]
        for neighbor in G.successors(node):
            paths = paths + get_down_stream_paths(neighbor)
        return paths

    return get_down_stream_paths(node_name)


def export_json_file(path, json_object):
    with open(path, "w") as outfile:
        outfile.write(json_object)


def filter_dictionary(dictionary):
    return {"operation": "glue", "trigger": "ALL", "properties": dictionary}


def create_graph_from_glue_jobs(glue_jobs):
    G = nx.Graph()
    G.add_node("start")
    G.nodes["start"]["metadata"] = {"trigger": "MANUAL", "operation": "NOOP"}

    for job in glue_jobs:
        G.add_node(job["Name"])
        G.nodes[job["Name"]]["metadata"] = filter_dictionary(job)

    G.add_node("stop")
    G.nodes["stop"]["metadata"] = {"operation": "NOOP"}
    return G
