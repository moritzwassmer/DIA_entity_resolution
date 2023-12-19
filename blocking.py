
# Spark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

def apply_bucket(df, bucket_function):

    """creates a new column by choosing the column """


    if bucket_function.__name__ == buckets_by_author.__name__:
        df["bucket"] =  df["Authors"].apply(bucket_function)
    elif bucket_function.__name__ == bucket_by_year.__name__:
        df["bucket"] =  df["Year"].apply(bucket_function)
    elif bucket_function.__name__ == buckets_by_author_spark.__name__:
        df = df.withColumn("bucket", bucket_function("Authors"))
    elif bucket_function.__name__ == bucket_by_year_spark.__name__:
        df = df.withColumn("bucket", bucket_function("Year"))
    else:
        raise NotImplementedError()
    return df

        



def buckets_by_author(authors:str):
    """
    takes MULTIPLE authors, takes first chars of first name and last name and creates a sorted list with unique characters
    
    eg "moritz wassmer, frederick williams" -> (f, m, w)
    """
    
    characters = set()
    
    for author in authors.split(', '): # name1, name 2:
        
        parts = author.split()
        
        if len(parts) < 2:
            characters.update({authors[0]}) # first character
        else:
            characters.update({parts[len(parts)-1][0]}) # letztes word, erster char
            characters.update({parts[0][0]}) # first word, first char
    
    #print(characters)
    characters = list(characters)#.sort()
    characters.sort()
    
    return ", ".join(characters)

def bucket_by_year(year:int):
    return year

buckets_by_author_spark = udf(buckets_by_author, StringType())

buckets_by_year_spark = udf(bucket_by_year, IntegerType())