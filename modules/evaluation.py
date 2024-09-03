# TODO transform to sql like: input dfs output 
def evaluate(truth:set[str], preds:set[str]): #, n:set[str]

    """
    takes truth and preds respectiveley, each being lists of  
    """
    
    tp = len(truth.intersection(preds))
    fn = len(preds - truth)
    fp = len(truth - preds)

    #tn =len(n) - fn # never used anyways

    # Display the results
    print("True Positives (TP):", tp)
    print("False Positives (FP):", fp)
    print("False Negatives (FN):", fn)
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    # Display the results
    print("Precision:", precision)
    print("Recall:", recall)
    print("F1-score:", f1)