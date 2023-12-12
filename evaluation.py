def evaluate(truth:set[str], preds:set[str]): #, n:set[str]

    """
    takes truth and preds respectiveley, each being lists of  
    """
    
    # Calculate True Positives (TP)
    tp = len(truth.intersection(preds))

    # Calculate False Positives (FP)
    fp = len(preds - truth)

    # Calculate False Negatives (FN)
    fn = len(truth - preds)

    # TN
    #tn =len(n) - fn # never used anyways

    # Display the results
    print("True Positives (TP):", tp)
    print("False Positives (FP):", fp)
    print("False Negatives (FN):", fn)
    #print("True Negatives (TN):", tn)
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0

    # Calculate recall
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0

    # Calculate F1-score
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    # Display the results
    print("Precision:", precision)
    print("Recall:", recall)
    print("F1-score:", f1)