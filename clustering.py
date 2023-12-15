import numpy as np
from helpers import *

def construct_adjacency_matrix(bucket_matched):
    # Step 1: Identify unique nodes
    unique_strings = get_unique_strings(bucket_matched)

    # Step 2: Create an empty adjacency matrix
    num_nodes = len(unique_strings)
    adj_matrix = np.zeros((num_nodes, num_nodes), dtype=int)

    # Step 3: Populate the adjacency matrix
    node_index = {node: index for index, node in enumerate(unique_strings)}

    for edge in bucket_matched:
        i, j = node_index[edge[0]], node_index[edge[1]]
        adj_matrix[i, j] = 1
        adj_matrix[j, i] = 1  # If the graph is undirected, include this line
    return adj_matrix

def get_connected_components(bucket_matched):

    """
    Args:
        bucket_matched: matched tuples (list of tuples of form (id_acm, id_dblp))

    Returns: 
        dictionary with every id as key and assigned clusters as value

    {'539087cf20f70186a0d5d01c': '53e9b47cb7602d9703f80ae7', '53e9b47cb7602d9703f80ae7': '53e9b47cb7602d9703f80ae7', '539087cf20f70186a0d5d01a': '53e9b41ab7602d9703f11e2a',
    """

    unique_strings = get_unique_strings(bucket_matched)

    # 1) generate adjacency matrix
    adj_matrix = construct_adjacency_matrix( bucket_matched)

        # Get the number of nodes
    num_nodes = adj_matrix.shape[0]

    # 2) generate initial clusters (own strings)
    node_dict = {}
    for node in unique_strings:
        node_dict[node] = node
    node_dict

    # 3) Fixed point iteration
    updated = True
    while updated:
        updated = False
        for i in range(num_nodes):     
            current_node = unique_strings[i]
            current_string = node_dict[current_node]

            # Get all neighboring strings
            neighboring_strings = [node_dict[unique_strings[j]] for j in range(num_nodes) if adj_matrix[i, j] == 1]

            # Assign the maximum value among the current string and neighboring strings
            new_string = max([current_string] + neighboring_strings)

            if new_string > current_string:
                node_dict[current_node] = new_string
                updated = True

    # Print the final node_dict
    #print("Final node_dict:")
    #print(node_dict)
    return node_dict

def resolve_df(matched, unmatched, clusters, filtered_acm_df, filtered_dblp_df):

    def toKeepHelper(index):
        return index in toKeep

    # 1) summarise clusters to 1 entity
    matched_kept = set(clusters.values())

    # 2) Extract unmatched ids
    unique_strings = get_unique_strings(matched)

    # Flatten the list of tuples
    flat_list = set([item for sublist in unmatched for item in sublist]).difference(unique_strings)

    # Get unique string values using set
    unique_strings_unmatched = list(flat_list)
    unmatched_kept = unique_strings_unmatched

    # 3) Extract rows which should be kept
    toKeep = set(matched_kept).union(set(unmatched_kept))

    filtered_acm_df["ToKeep"] = filtered_acm_df["Index"].apply(toKeepHelper)
    filtered_dblp_df["ToKeep"] = filtered_dblp_df["Index"].apply(toKeepHelper)

    result_acm = filtered_acm_df.loc[filtered_acm_df["ToKeep"]==True]
    result_dblp = filtered_dblp_df.loc[filtered_dblp_df["ToKeep"]==True]

    total_result = pd.concat([result_acm, result_dblp])

    return total_result