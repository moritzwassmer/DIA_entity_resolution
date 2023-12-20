# Blocking, matching
import Levenshtein
import pandas as pd
from tqdm import tqdm
from itertools import  product

from params import *

from pyspark.sql.functions import expr, col

def levenshtein(str1:str, str2:str):

    """
    compare 2 strings based on Levenshtein distance
    """


    # Calculate Levenshtein distance
    distance = Levenshtein.distance(str1, str2)

    # Calculate Levenshtein similarity (normalized similarity score)
    similarity = 1 - distance / max(len(str1), len(str2))
    
    return similarity


def simple_similarity(row1:pd.Series, row2:pd.Series):

    """
    averaged similarity based on 3 attributes
    """

    authors_sim = levenshtein(row1["Authors"], row2["Authors"])
    title_sim = levenshtein(row1["Title"], row2["Title"])
    venue_sim = levenshtein(row1["Venue"], row2["Venue"])
    year_sim = row1["Year"] == row2["Year"]
    return (authors_sim+ title_sim + venue_sim + year_sim)/4

def exact_match(row1:pd.Series, row2:pd.Series): 
    """
    exact match of attributes
    """

    authors_sim = row1["Authors"] == row2["Authors"]
    title_sim = row1["Title"] == row2["Title"]
    venue_sim = row1["Venue"] == row2["Venue"]
    year_sim = row1["Year"] == row2["Year"]
    return authors_sim & title_sim & venue_sim & year_sim

# 1) year : should be exactly the same. if +-1 is maybe still a match if everything else is a match
# 2) author : i could imagine middle names could be missing sometimes and sometimes not -> only compare first character of firs and last name eg. "m. tamer özsu"
# 3) venue : just check if both contain sigmod/vldb 
# 4) title : levenstein/jaccard > 0.9 should do

def fancy_similarity(row1:pd.Series, row2:pd.Series):
    year_sim = row1["Year"] == row2["Year"]
    authors_sim = levenshtein(row1["Authors_new"], row2["Authors_new"]) > 0.9
    title_sim = levenshtein(row1["Title"], row2["Title"]) > 0.9
    venue_sim = True if row1["isSigmod"] and row1["isSigmod"] or row1["isVLDB"] and row1["isVLDB"] else False
    return title_sim & authors_sim & venue_sim & year_sim

def match_by_bucket(df1:pd.DataFrame, df2:pd.DataFrame, similarity_function, threshold:float=1, sfx_1="_acm", sfx_2="_dblp"):
    
    """
    matches the rows by using the similarity funciton. 
    similarity function takes 2 pandas rows row1:pd.Series, row2:pd.Series

    Return:
        List of tuples of indices [(a,b), (a,c)]
    """

    matched_pairs = list()
    unmatched_pairs = list()
    
    unique_buckets = set(df1["bucket"]).union(set(df2["bucket"]))

    for bucket in tqdm(unique_buckets):

        df1_bucket = df1[df1['bucket'] == bucket].reset_index()
        df2_bucket = df2[df2['bucket'] == bucket].reset_index()

        for ix1, ix2 in product(list(df1_bucket.index),list(df2_bucket.index)): # each combination

            row1 = df1_bucket.iloc[ix1]
            row2 = df2_bucket.iloc[ix2]

            pair = tuple(row1[1:]) + tuple(row2[1:])
            if similarity_function(row1, row2) >= threshold: 
                matched_pairs += [pair]
            else:
                unmatched_pairs += [pair]

    #unmatched_pairs = get_unmatched(matched_df["Index"], unmatched_df) # TODO not clean
                
    colnames = [item + sfx_1 for item in list(df1.columns)] +  [item + sfx_2 for item in list(df1.columns)]
    
    
    matched_df = pd.DataFrame(matched_pairs, columns=colnames)
    unmatched_df = pd.DataFrame(unmatched_pairs, columns=colnames)


                     
    return matched_df, unmatched_df #+ list(unmatched)

def match_without_bucket(df1:pd.DataFrame, df2:pd.DataFrame, similarity_function, threshold:float=1, sfx_1="_acm", sfx_2="_dblp"):
    """
    matches the rows by using the similarity funciton without using the bucket column in the dataframe 

    Return:
        List of tuples of indices [(a,b), (a,c)]
    """

    combinations = list(product(list(df1.index),list(df2.index)))
    matched_pairs = list()
    unmatched_pairs = list()
    
    for ix1, ix2 in tqdm(combinations): # each combination

        row1 = df1.iloc[ix1]
        row2 = df2.iloc[ix2]

        pair = tuple(row1) + tuple(row2)
        if similarity_function(row1, row2) >= threshold: 
            matched_pairs += [pair]
        else:
            unmatched_pairs += [pair]

    colnames = [item + sfx_1 for item in list(df1.columns)] +  [item + sfx_2 for item in list(df1.columns)]
    
    matched_df = pd.DataFrame(matched_pairs, columns=colnames)
    unmatched_df = pd.DataFrame(unmatched_pairs, columns=colnames)

    return matched_df, unmatched_df #= list() #+ list(unmatched)

def matching_spark(df1, df2, similarity_expression, threshold=1, sfx_1="_acm", sfx_2="_dblp"): 

    # 1) Add suffixes "_acm" and "_dblp" to the column names before join
    df1 = df1.select(
        [col(c).alias(c + sfx_1) for c in df1.columns]
    )
    df2 = df2.select(
        [col(c).alias(c + sfx_2)  for c in df2.columns]
    )

    # 2) a) Calculate Matched rows
    df1 = df1.repartition("bucket_acm").alias("acm") # hardcoded
    df2 = df2.repartition("bucket_dblp").alias("dblp")

    bucket_matched_df = df1.join(
        df2,
        expr(similarity_expression),
        how="inner"
    )

    # 2) b) Calculate Unmatched rows 
    similarity_expression_anti = f"NOT {similarity_expression}"
    bucket_unmatched_df = df1.join(
        df2,
        expr(similarity_expression_anti),
        how="inner"
    )

    return bucket_matched_df, bucket_unmatched_df 

# TODO implement RDD version of matching_spark