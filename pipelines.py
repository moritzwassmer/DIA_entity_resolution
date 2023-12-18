# TODO add final_df in return
from blocking import buckets_by_author, buckets_by_author_spark
from matching import *
from clustering import *
from params import *
from pyspark.sql.functions import  col

def er_pipeline(matching_similarity, acm, dblp, return_df = False, bucket_function=buckets_by_author, threshold=1):

    # 1) Blocking
    acm["bucket"] =  acm["Authors"].apply(bucket_function)
    dblp["bucket"] =  dblp["Authors"].apply(bucket_function)

    # 2) Matching
    bucket_matched_df, bucket_unmatched_df = match_by_bucket(acm, dblp, similarity_function=matching_similarity, threshold = threshold)
    bucket_matched = df_to_tuples(bucket_matched_df[["Index_acm", "Index_dblp"]])
    bucket_unmatched = df_to_tuples(bucket_unmatched_df[["Index_acm", "Index_dblp"]])

    # 3) Clustering
    clusters = get_connected_components(bucket_matched)
    final_df = resolve_df(bucket_matched, bucket_unmatched, clusters, acm, dblp)

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
    final_df = resolve_df(unbucket_matched, unbucket_unmatched, clusters, acm, dblp)

    if return_df:
        return unbucket_matched_df, unbucket_unmatched_df
    else:
        return unbucket_matched, unbucket_unmatched
    
def spark_pipeline(matching_similarity, acm, dblp, return_df = False, bucket_function=buckets_by_author_spark, threshold=1):

    # 1) Blocking
    acm = acm.withColumn("bucket", bucket_function("Authors"))
    dblp = dblp.withColumn("bucket", bucket_function("Authors"))

    # 2) Matching
    bucket_matched_df, bucket_unmatched_df = matching_spark(acm, dblp, matching_similarity, threshold=threshold)

    # 3) Clustering
    # retrieve indices
    bucket_matched = bucket_matched_df.select([col("Index_acm"), col("Index_dblp")]).dropDuplicates()
    bucket_unmatched = bucket_unmatched_df.select([col("Index_acm"), col("Index_dblp")]).dropDuplicates()
 
    # convert to tuples # TODO Causes Py4J Error when data size large
    bucket_matched = df_to_tuples(bucket_matched, False) 
    #bucket_unmatched = df_to_tuples(bucket_unmatched, False) 

    # clustering - # TODO Nicht parallel
    #clusters = get_connected_components(bucket_matched) 
    #final_df = resolve_df(bucket_matched_df.toPandas(), bucket_unmatched_df.toPandas(), clusters, acm.toPandas(), dblp.toPandas())

    if return_df:
        return bucket_matched_df, bucket_unmatched_df
    else:
        return bucket_matched,  bucket_unmatched # TODO Causes crash