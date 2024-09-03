"""
# Entity Resolution Pipeline 

This project aims to create an Entity Resolution (ER) pipeline for the deduplication of the ACM and DBLP publications datasets. The project was developed as part of the 2023/2024 course "Data Integration and Large-scale Analysis". 

## Problem Description
The ACM and DBLP dataset both consist of publications. Some of these publications are only published on one of the datasets, while others are contained in both. For those contained in both, it is not guaranteed that both values match exactly. Therefore a heuristical approach has to be found to decide which of the entries in each dataset refer to the same publication. In the end, we want to keep 1 entry for each publication to create an integrated dataset.

To test the scalability of the approach, the datasets were replicated up to 10 times.

### Computational complexity
For the matching procedure without blocking, the computational complexity of the matching procedure is $O(R^2 \cdot N \cdot M)$
, with R being the replication factor (up to 10), N being the number of rows of the ACM dataset, and M being the number of rows of the DBLP dataset. 

### Approach
The approach selected involves 3 stages. For Blocking and Matching, multiple techniques were compared.

#### 1) Blocking
Reduce the computational complexity by limiting the amount of matches performed by only comparing rows with the same 'bucket'.
-	Baseline (Naïve Brutue Force, no buckets):
-	Blocking by year:
Buckets are constructed by the original Year columns. 
-	Blocking by year and venue:
Buckets are constructed by the original Year columns with appended substring ‘sigmod’ when there is “sigmod” present or “dldp” when dblp is present. 
-	Blocking by authors:
Buckets are constructed by creating a sorted list without duplicates of characters according to the following scheme: First character of First name, First character of Last name. 
For example: “moritz wassmer, frederick klaus meier” -> [f, m, w]

#### 2) Matching
Match rows of both datasets and calculate a similarity to determine if they refer to the same entity. Similarity measures used are the following:
-	Exact Match
This similarity is simply checking if all attributes are the same in both rows.
-	Simple Similarity
This similarity measure calculates the distance between each attribute from each dataset and takes an average which is thresholded to determine a match:
It uses levenstein distance for string attributes, and checks wether years in both rows are the same (which is represented by similarity 1, else 0).
-	Fancy Similarity
This similarity measure is not thresholded and was constructed in the following way:
Year has to be the same
Authors levenstein distance has to be above 0.9
Title levenstein distance has to be above 0.9
Venue have to both contain substring sigmod or vldb

#### 3) Clustering 
Group entities that are similar and keep 1 entry

## Installation

1) Please refer to the `requirements.txt` file for the necessary dependencies.
2) download the datasets https://lfs.aminer.cn/lab-datasets/citation/dblp.v8.tgz and https://lfs.aminer.cn/lab-datasets/citation/citation-acm-v8.txt.tgz and put them into folder `in`
3) for the pyspark implementation of `2_3_entity_resolution.ipynb` an installation of spark is required
https://www.datacamp.com/tutorial/installation-of-pyspark

## Usage

To use this project, follow these steps:
1. Open and run the `1_data_acquisition_preparation.ipynb` notebook.
2. Open and run the `2_3_entity_resolution.ipynb` notebook.
