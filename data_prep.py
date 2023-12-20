import pandas as pd
from tqdm import tqdm

def cast_df(df:pd.DataFrame):

    """

    Casts dataframes column types like below:

    Title      string[python]
    Authors    string[python]
    Year                Int64
    Venue      string[python]
    Index      string[python]
    """

    df['Year'] = df['Year'].astype('Int64')  # Use 'Int64' to allow for NaN values in integer column
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
            #current_record.setdefault('Authors', []).append(line[2:].strip())
            authors_string = line[2:].strip()
            current_record['Authors'] = authors_string#.setdefault('Authors', []).extend(authors_string.split(', '))
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
    
    # explode authors column
    #df['Authors'] = df['Authors'].apply(lambda x: [] if pd.isna(x) else x)  # Replace NaN with empty list
    #df = df.explode('Authors').reset_index(drop=True)

    #  type casting
    df = cast_df(df)

    return df