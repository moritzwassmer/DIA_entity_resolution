from helpers import *
import pandas as pd


def get_connected_components(bucket_matched):
    """
    Args:
        bucket_matched: matched tuples (list of tuples of form (id_acm, id_dblp))

    Returns: 
        dictionary with every id as key and assigned clusters as value

        {'539087cf20f70186a0d5d01c': '53e9b47cb7602d9703f80ae7', '53e9b47cb7602d9703f80ae7': '53e9b47cb7602d9703f80ae7', '539087cf20f70186a0d5d01a': '53e9b41ab7602d9703f11e2a',
    """

    unique_strings = get_unique_strings(bucket_matched)

    # 1) generate edge list
    edge_list = bucket_matched

    # 2) generate initial clusters (own strings)
    node_dict = {}
    for node in unique_strings:
        node_dict[node] = node

    # 3) Fixed point iteration
    updated = True
    while updated:
        updated = False
        for edge in edge_list:
            node_i, node_j = edge

            # Get strings associated with nodes
            string_i, string_j = node_dict[node_i], node_dict[node_j]

            # Assign the maximum value among the current strings
            new_string = max(string_i, string_j)

            if new_string > string_i:
                node_dict[node_i] = new_string
                updated = True
            if new_string > string_j:
                node_dict[node_j] = new_string
                updated = True

    return node_dict

def resolve_df(matched, unmatched, clusters, filtered_acm_df, filtered_dblp_df):

    def toKeepHelper(index):
        return index in toKeep

    # 1) summarise clusters to 1 entity
    matched_kept = set(clusters.values())

    # 2) Extract rows which should be kept
    toKeep = set(matched_kept).union(set(unmatched))

    filtered_acm_df["ToKeep"] = filtered_acm_df["Index"].apply(toKeepHelper)
    filtered_dblp_df["ToKeep"] = filtered_dblp_df["Index"].apply(toKeepHelper)

    result_acm = filtered_acm_df.loc[filtered_acm_df["ToKeep"]==True]
    result_dblp = filtered_dblp_df.loc[filtered_dblp_df["ToKeep"]==True]

    total_result = pd.concat([result_acm, result_dblp])

    return total_result