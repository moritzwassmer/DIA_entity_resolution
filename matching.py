# Blocking, matching
import Levenshtein

def similar_score_str(str1, str2):
    # Calculate Levenshtein distance
    distance = Levenshtein.distance(str1, str2)

    # Calculate Levenshtein similarity (normalized similarity score)
    similarity = 1 - distance / max(len(str1), len(str2))
    
    return similarity

def total_similarity(row1, row2):
    #authors_sim = similar_score_str(row1["Authors"], row2["Authors"])
    title_sim = similar_score_str(row1["Title"], row2["Title"])
    venue_sim = similar_score_str(row1["Venue"], row2["Venue"])
    year_sim = row1["Year"] == row2["Year"]
    return ( title_sim + venue_sim + year_sim)/3

def exact_match(row1, row2):
    #authors_sim = row1["Authors"] == row2["Authors"]
    title_sim = row1["Title"] == row2["Title"]
    venue_sim = row1["Venue"] == row2["Venue"]
    year_sim = row1["Year"] == row2["Year"]
    return title_sim & venue_sim & year_sim