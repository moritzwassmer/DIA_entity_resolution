# Blocking, matching
import Levenshtein
import pandas as pd
from tqdm import tqdm
from itertools import combinations, product

from clustering import get_unique_strings

def similar_score_str(str1:str, str2:str):

    """
    compare 2 strings based on Levenshtein distance
    """


    # Calculate Levenshtein distance
    distance = Levenshtein.distance(str1, str2)

    # Calculate Levenshtein similarity (normalized similarity score)
    similarity = 1 - distance / max(len(str1), len(str2))
    
    return similarity

def total_similarity(row1:pd.Series, row2:pd.Series):

    """
    averaged similarity based on 3 attributes
    """

    #authors_sim = similar_score_str(row1["Authors"], row2["Authors"])
    title_sim = similar_score_str(row1["Title"], row2["Title"])
    venue_sim = similar_score_str(row1["Venue"], row2["Venue"])
    year_sim = row1["Year"] == row2["Year"]
    return (title_sim + venue_sim + year_sim)/3

def exact_match(row1:pd.Series, row2:pd.Series): # not used

    """
    exact match of attributes
    """

    #authors_sim = row1["Authors"] == row2["Authors"]
    title_sim = row1["Title"] == row2["Title"]
    venue_sim = row1["Venue"] == row2["Venue"]
    year_sim = row1["Year"] == row2["Year"]
    return title_sim & venue_sim & year_sim


# TODO Write in SQL like commands, hardcoded 
# SOLVED, but not tested probably wrong. i need to guarantee that row is only added to unmatched if there couldnt be found a match at all
# TODO return dataframes with full rows instead of only ids

def match_by_bucket(df1:pd.DataFrame, df2:pd.DataFrame, similarity_function, threshold:float=1):
    
    """
    matches the rows by using the similarity funciton. 
    similarity function takes 2 pandas rows row1:pd.Series, row2:pd.Series

    Return:
        List of tuples of indices [(a,b), (a,c)]
    """

    matched_ids = list()
    unmatched_ids = list()
    
    unique_buckets = set(df1["bucket"]).union(set(df2["bucket"]))

    for bucket in tqdm(unique_buckets):

        df1_bucket = df1[df1['bucket'] == bucket].reset_index()
        df2_bucket = df2[df2['bucket'] == bucket].reset_index()

        for ix1, ix2 in product(list(df1_bucket.index),list(df2_bucket.index)): # each combination

            row1 = df1_bucket.iloc[ix1]
            row2 = df2_bucket.iloc[ix2]

            if similarity_function(row1, row2) >= threshold: 
                matched_ids += [(row1["Index"], row2["Index"])]
            else:
                unmatched_ids += [(row1["Index"], row2["Index"])]

    unmatched_ids = get_unmatched(matched_ids, unmatched_ids) # TODO not clean
                     
    return matched_ids, unmatched_ids #+ list(unmatched)

def get_unmatched(matched_ids, unmatched_ids):
    unique_strings = get_unique_strings(matched_ids)

    # Flatten the list of tuples
    flat_list = set([item for sublist in unmatched_ids for item in sublist]).difference(unique_strings)

    # Get unique string values using set
    unique_strings_unmatched = list(flat_list)
    return unique_strings_unmatched

def match_without_bucket(df1:pd.DataFrame, df2:pd.DataFrame, similarity_function, threshold:float=1):
    """
    matches the rows by using the similarity funciton without using the bucket column in the dataframe 

    Return:
        List of tuples of indices [(a,b), (a,c)]
    """

    combinations = list(product(list(df1.index),list(df2.index)))
    matched_ids = list()
    unmatched_ids = list()
    
    for ix1, ix2 in tqdm(combinations): # each combination

        row1 = df1.iloc[ix1]
        row2 = df2.iloc[ix2]

        if similarity_function(row1, row2) >= threshold: # TODO see todo above
            matched_ids += [(row1["Index"], row2["Index"])]
        else:
            unmatched_ids += [(row1["Index"], row2["Index"])]

    unmatched_ids = get_unmatched(matched_ids, unmatched_ids) # TODO not clean

    return matched_ids, unmatched_ids #= list() #+ list(unmatched)