# TODO add final_df in return
from .blocking import *
from .matching import *
from .clustering import *
from .params import *
from pyspark.sql.functions import  col
from .helpers import *

import time

def extract_unmatched(matched, unmatched): # TODO make more readable
    ### EXTRACT UNMATCHED IDS
    # 2) Extract unmatched ids
    unique_strings = get_unique_strings(matched)

    # Flatten the list of tuples
    flat_list = set([item for sublist in unmatched for item in sublist]).difference(unique_strings)

    # Get unique string values using set
    unmatched_kept = list(flat_list)

    return unmatched_kept

def extract_unmatched_2(matched, acm, dblp): # matched is list of tuples, acm and dblp pandas dataframes
    ### EXTRACT UNMATCHED IDS
    # 2) Extract unmatched ids
    unique_strings = get_unique_strings(matched)

    unmatched_kept = set(acm["Index"].tolist()).union(set(dblp["Index"].tolist())).difference(unique_strings)

    return unmatched_kept

def extract_unmatched_2_spark(matched, acm, dblp):
    pass # TODO



def er_pipeline(matching_similarity, acm, dblp, return_df = False, bucket_function=buckets_by_author, threshold=1, return_final_df=False):

    # 1) Blocking
    acm = apply_bucket(acm, bucket_function)
    dblp =  apply_bucket(dblp, bucket_function)

    # 2) Matching
    bucket_matched_df, bucket_unmatched_df = match_by_bucket(acm, dblp, similarity_function=matching_similarity, threshold = threshold)
    bucket_matched = df_to_tuples(bucket_matched_df[["Index_acm", "Index_dblp"]])
    bucket_unmatched = df_to_tuples(bucket_unmatched_df[["Index_acm", "Index_dblp"]])

    # 3) Clustering
    clusters = get_connected_components(bucket_matched)

    unmatched_kept = extract_unmatched_2(bucket_matched, acm, dblp)
    final_df = resolve_df(bucket_matched, unmatched_kept, clusters, acm, dblp)

    if return_final_df:
        return final_df
    
    if return_df:
        return bucket_matched_df, bucket_unmatched_df
    else:
        return bucket_matched, bucket_unmatched

def baseline_pipeline(matching_similarity, acm, dblp, return_df = False, threshold=1):

    # 1) Matching
    unbucket_matched_df, unbucket_unmatched_df = match_without_bucket(acm, dblp, similarity_function=matching_similarity, threshold = threshold) # TODO RUNTIME
    unbucket_matched = df_to_tuples(unbucket_matched_df[["Index_acm", "Index_dblp"]])
    unbucket_unmatched = df_to_tuples(unbucket_unmatched_df[["Index_acm", "Index_dblp"]])
    
    # 2) Clustering
    clusters = get_connected_components(unbucket_matched)
    unmatched_kept = extract_unmatched_2(unbucket_matched, acm, dblp)
    final_df = resolve_df(unbucket_matched, unmatched_kept, clusters, acm, dblp)

    if return_df:
        return unbucket_matched_df, unbucket_unmatched_df
    else:
        return unbucket_matched, unbucket_unmatched
    
def spark_pipeline(matching_similarity, acm, dblp, return_df = False, bucket_function=buckets_by_author_spark, threshold=1):

    # 1) Blocking
    acm = apply_bucket(acm, bucket_function)
    dblp =  apply_bucket(dblp, bucket_function)

    # 2) Matching
    bucket_matched_df, bucket_unmatched_df = matching_spark(acm, dblp, matching_similarity, threshold=threshold)

    # 3) Clustering
    # retrieve indices
    start_time = time.time()
    bucket_matched_index_df = bucket_matched_df.select([col("Index_acm"), col("Index_dblp")])#.dropDuplicates()
    bucket_unmatched = bucket_unmatched_df.select([col("Index_acm"), col("Index_dblp")])#.dropDuplicates()

    bucket_matched = df_to_tuples(bucket_matched_index_df, False) 
    #bucket_unmatched = df_to_tuples(bucket_unmatched, False) #, bucket_unmatched is too big for the replication experiments and would cause a crash

    end_time = time.time()
    er_time = round(end_time - start_time,2)

    print("matching time:"+ str(er_time))

    # clustering - Spark not used # TODO SPARK IMPLEMENTATION
    start_time = time.time()
    clusters = get_connected_components(bucket_matched) 
    end_time = time.time()
    er_time = round(end_time - start_time,2)

    print("clustering time:"+ str(er_time))

    start_time = time.time()
    acm_pd, dblp_pd = acm.toPandas(), dblp.toPandas()
    bucket_unmatched_kept = extract_unmatched_2(bucket_matched, acm_pd, dblp_pd) 
    final_df = resolve_df(bucket_matched, bucket_unmatched_kept, clusters, acm_pd, dblp_pd)

    end_time = time.time()
    er_time = round(end_time - start_time,2)

    print("resolving time:"+ str(er_time))

    if return_df:
        return bucket_matched_df#, bucket_unmatched_df
    else:
        return bucket_matched#,  bucket_unmatched is too big for the replication experiments and would cause a crash