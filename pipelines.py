# TODO add final_df in return
from blocking import buckets_by_author
from matching import *
from clustering import *
from params import *

def er_pipeline(matching_similarity, acm, dblp, return_df = False, bucket_function=buckets_by_author):

    # 1) Blocking
    acm["bucket"] =  acm["Authors"].apply(bucket_function)
    dblp["bucket"] =  dblp["Authors"].apply(bucket_function)

    # 2) Matching
    bucket_matched_df, bucket_unmatched_df = match_by_bucket(acm, dblp, similarity_function=matching_similarity, threshold = 0.9)
    bucket_matched = df_to_tuples(bucket_matched_df[["Index_acm", "Index_dblp"]])
    bucket_unmatched = df_to_tuples(bucket_unmatched_df[["Index_acm", "Index_dblp"]])

    # 3) Clustering
    clusters = get_connected_components(bucket_matched)
    final_df = resolve_df(bucket_matched, bucket_unmatched, clusters, acm, dblp)

    if return_df:
        return bucket_matched_df, bucket_unmatched_df
    else:
        return bucket_matched, bucket_unmatched

    

def baseline_pipeline(matching_similarity, acm, dblp, return_df = False):

    # 1) Matching
    unbucket_matched_df, unbucket_unmatched_df = match_without_bucket(acm, dblp, similarity_function=matching_similarity, threshold = 0.9) # TODO RUNTIME
    unbucket_matched = df_to_tuples(unbucket_matched_df[["Index_acm", "Index_dblp"]])
    unbucket_unmatched = df_to_tuples(unbucket_unmatched_df[["Index_acm", "Index_dblp"]])
    
    # 2) Clustering
    clusters = get_connected_components(unbucket_matched)
    final_df = resolve_df(unbucket_matched, unbucket_unmatched, clusters, acm, dblp)

    if return_df:
        return unbucket_matched_df, unbucket_unmatched_df
    else:
        return unbucket_matched, unbucket_unmatched