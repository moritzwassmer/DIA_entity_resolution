
# Spark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

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
    
    #print(characters)
    characters = list(characters)#.sort()
    characters.sort()
    
    return ", ".join(characters)

def bucket_by_year(year:int):
    return year

buckets_by_author_spark = udf(buckets_by_author, StringType())