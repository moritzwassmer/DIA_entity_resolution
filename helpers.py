import pandas as pd

def get_unique_strings(bucket_matched):

    """
    list of tuples (str, str) ->  list of unique strings within list
    """

    flat_list = [item for sublist in bucket_matched for item in sublist]

    # Get unique string values using set
    unique_strings = flat_list
    return unique_strings

def df_to_tuples(df, pandas=True):
    if pandas:
        return [tuple(row) for row in df.itertuples(index=False, name=None)]
    else:
        return df.rdd.map(tuple).collect() # TODO check class