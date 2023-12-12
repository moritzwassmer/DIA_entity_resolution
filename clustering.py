import numpy as np

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

def get_unique_strings(bucket_matched):
    flat_list = [item for sublist in bucket_matched for item in sublist]

    # Get unique string values using set
    unique_strings = flat_list
    return unique_strings

def get_connected_components(bucket_matched):

    unique_strings = get_unique_strings(bucket_matched)

    # 1) generate adjacency matrix
    adj_matrix = construct_adjacency_matrix( bucket_matched)

    # 2) generate initial clusters (own strings)
    node_dict = {}
    for node in unique_strings:
        node_dict[node] = node
    node_dict

    # Get the number of nodes
    num_nodes = adj_matrix.shape[0]

    # Function to check and update node_dict
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
    print("Final node_dict:")
    print(node_dict)
    return node_dict