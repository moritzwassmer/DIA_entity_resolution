import pandas as pd
import numpy as np

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
        df = df.toPandas()
        return df_to_tuples(df, pandas=True)#df.rdd.map(tuple).collect() # TODO check class
    
def modify_columns(row): # TODO
    # Modify Title, Authors, and Venue by changing 1 to 3 randomly selected characters
    if len(row['Title']) > 3:
        chars_to_change_title = np.random.choice(len(row['Title']), size=np.random.randint(1, 4), replace=False)
        row['Title'] = ''.join([c if i not in chars_to_change_title else np.random.choice(list(row['Title'])) for i, c in enumerate(row['Title'])])

    """if len(row['Authors']) > 3:
        chars_to_change_authors = np.random.choice(len(row['Authors']), size=np.random.randint(1, 4), replace=False)
        row['Authors'] = ''.join([c if i not in chars_to_change_authors else np.random.choice(list(row['Authors'])) for i, c in enumerate(row['Authors'])])
    """
    if len(row['Venue']) > 3:
        chars_to_change_venue = np.random.choice(len(row['Venue']), size=np.random.randint(1, 4), replace=False)
        row['Venue'] = ''.join([c if i not in chars_to_change_venue else np.random.choice(list(row['Venue'])) for i, c in enumerate(row['Venue'])])
    
    """# Modify Index by changing 1 to 3 randomly selected characters
    if len(row['Index']) > 3:
        chars_to_change_index = np.random.choice(len(row['Index']), size=np.random.randint(1, 4), replace=False)
        row['Index'] = ''.join([c if i not in chars_to_change_index else np.random.choice(list(row['Index'])) for i, c in enumerate(row['Index'])])
    """
    # Randomly decide to add 1 year to Year
    if np.random.choice([True, False]):
        row['Year'] += np.random.choice([0,1])

    return row

def modify_and_concat(df, num_iterations):
    modified_dfs = []

    for _ in range(num_iterations):
        # Apply modifications to specified columns
        df_modified = df.apply(modify_columns, axis=1)
        modified_dfs.append(df_modified)

    # Concatenate the modified DataFrames
    result_df = pd.concat(modified_dfs, ignore_index=True)

    return result_df