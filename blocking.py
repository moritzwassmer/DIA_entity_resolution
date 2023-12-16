
# Spark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def extract_word_character(author:str): # not applicable anymore for current database scheme, was used for normalized authors
    """
    takes SINGLE author. eg "moritz wassmer" -> (m, w)
    """
    words = author.split()
    tpl = tuple()
    num_words = len(words)

    if num_words < 2:
        tpl = (words[0][0], "") # first word first character
    else:
        tpl = (words[0][0], words[num_words-1][0]) # last word last character
    return tpl

def buckets_by_author(authors:str):
    """
    takes MULTIPLE authors, takes first chars of first name and last name and creates a sorted list with unique characters
    
    eg "moritz wassmer, frederick williams" -> (f, m, w)
    """
    
    characters = set()
    
    for author in authors.split(', '): # name1, name 2:
        
        parts = author.split()
        
        if len(parts) < 2:
            characters.update({parts[0]}) # first character # TODO was parts[0][0]
        else:
            characters.update({parts[len(parts)-1][0]}) # letztes word, erster char
    
    #print(characters)
    characters = list(characters)#.sort()
    characters.sort()
    
    return ", ".join(characters)

buckets_by_author_spark = udf(buckets_by_author, StringType())