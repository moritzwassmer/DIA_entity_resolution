import pandas as pd
from tqdm import tqdm

def cast_df(df:pd.DataFrame):

    df['Year'] = df['Year'].astype('Int64')  
    df['Venue'] = df['Venue'].astype('string')
    df['Index'] = df['Index'].astype('string')
    df['Title'] = df['Title'].astype('string')
    df['Authors'] = df['Authors'].astype('string')

    # strings to lower
    df['Venue'] = df['Venue'].str.lower()
    df['Title'] = df['Title'].str.lower()
    df['Authors'] = df['Authors'].str.lower()

    return df



def parse_text(lines):

    """

    Extracts Title, Author, Year, Venue and Index into a dataframe 

    #* --- paperTitle
    #@ --- Authors
    #t ---- Year
    #c  --- publication venue
    #index 00---- index id of this paper
    #% ---- the id of references of this paper (there are multiple lines, with each indicating a reference)
    #! --- Abstract

    """

    records = []

    current_record = {}
    for line in tqdm(lines):
        line = line.strip()

        if line.startswith('#*'):
            current_record['Title'] = line[2:].strip()
        elif line.startswith('#@'):
            authors_string = line[2:].strip()
            current_record['Authors'] = authors_string
        elif line.startswith('#t'):
            current_record['Year'] = line[2:].strip()
        elif line.startswith('#c'):
            current_record['Venue'] = line[2:].strip()
        elif line.startswith('#index'):
            current_record['Index'] = line[6:].strip()

        # Assuming records are separated by an empty line
        elif not line:
            records.append(current_record)
            current_record = {}
    
    # dataframe
    df = pd.DataFrame(records, columns=['Title', 'Authors', 'Year', 'Venue', 'Index'])

    #  type casting
    df = cast_df(df)

    return df