"""
# Entity Resolution Pipeline

An entity resolution and deduplication pipeline for the ACM and DBLP publications datasets using PySpark and advanced blocking/matching techniques.

## Overview

This project implements a scalable Entity Resolution (ER) pipeline designed to deduplicate and integrate publication records from the ACM and DBLP datasets. Developed as part of the 2023/2024 course "Data Integration and Large-scale Analysis", the pipeline addresses the challenge of identifying duplicate publications across heterogeneous data sources where exact matches are not guaranteed.

## Features

- **Multiple Blocking Strategies**: Baseline brute force, year-based, venue-based, and author-based blocking
- **Flexible Similarity Measures**: Exact match, simple similarity (Levenshtein distance), and fancy similarity (threshold-based)
- **Scalable Architecture**: PySpark implementation handles datasets replicated up to 10x
- **Modular Design**: Organized into reusable modules for blocking, matching, clustering, and evaluation
- **Performance Evaluation**: Built-in evaluation metrics and execution time analysis

## Prerequisites

- Python 3.8+
- Apache Spark 3.x (correctly installed and configured)
- Conda or Miniconda (for environment management)

**Important**: PySpark requires a proper Apache Spark installation. Refer to the [official Spark documentation](https://spark.apache.org/docs/latest/) for installation instructions specific to your operating system.

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd DIA_entity_resolution
   ```

2. **Install Apache Spark**
   
   Follow the official installation guide for your platform:
   - [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
   - [PySpark Installation Tutorial](https://www.datacamp.com/tutorial/installation-of-pyspark)
   
   Ensure that `SPARK_HOME` and `JAVA_HOME` environment variables are properly configured.

3. **Create the conda environment**
   ```bash
   conda env create -f environment.yaml
   conda activate entity-resolution
   ```

4. **Download the datasets**
   
   Download the following datasets and place them in the `in/` folder:
   - [DBLP dataset](https://lfs.aminer.cn/lab-datasets/citation/dblp.v8.tgz)
   - [ACM dataset](https://lfs.aminer.cn/lab-datasets/citation/citation-acm-v8.txt.tgz)
   
   Extract the archives so that the data files are directly accessible in the `in/` directory.

## Usage

Run the notebooks in sequence:

1. **Data Acquisition and Preparation**
   ```bash
   jupyter notebook 1_data_acquisition_preparation.ipynb
   ```
   This notebook handles data loading, cleaning, and preprocessing.

2. **Entity Resolution**
   ```bash
   jupyter notebook 2_3_entity_resolution.ipynb
   ```
   This notebook executes the blocking, matching, and clustering pipeline.

Alternatively, explore individual implementations:
- `ER.ipynb` - Alternative entity resolution approaches
- `pyspark_test.ipynb` - PySpark configuration testing

## Project Structure

```
DIA_entity_resolution/
├── modules/                    # Core pipeline modules
│   ├── __init__.py
│   ├── blocking.py            # Blocking strategies
│   ├── clustering.py          # Entity clustering algorithms
│   ├── data_prep.py           # Data preparation utilities
│   ├── evaluation.py          # Evaluation metrics
│   ├── helpers.py             # Helper functions
│   ├── matching.py            # Similarity measures
│   ├── params.py              # Configuration parameters
│   └── pipelines.py           # End-to-end pipelines
├── in/                        # Input datasets (not included)
├── 1_data_acquisition_preparation.ipynb
├── 2_3_entity_resolution.ipynb
├── ER.ipynb
├── pyspark_test.ipynb
├── environment.yaml           # Conda environment specification
└── README.md                  # This file
``` 

## Problem Description

The ACM and DBLP datasets both contain scholarly publication records. While some publications exist exclusively in one dataset, many appear in both. However, duplicate entries across datasets often have inconsistencies in attribute values (titles, authors, venues, etc.), making exact matching infeasible. This project develops a heuristic approach to identify which entries refer to the same publication, ultimately producing an integrated dataset with one entry per unique publication.

To test scalability, datasets were replicated up to 10 times.

### Example Data

![ACM Example](https://github.com/user-attachments/assets/893031c8-8f8d-4b3a-be85-38748d13ff88)
![DBLP Example](https://github.com/user-attachments/assets/b9be0bbd-3f62-4332-b202-96b3b8b78da8)

### Computational Complexity

Without blocking, the computational complexity of the matching procedure is:

$$O(R^2 \cdot N \cdot M)$$

where:
- $R$ = replication factor (up to 10)
- $N$ = number of ACM records
- $M$ = number of DBLP records

## Methodology

The entity resolution pipeline consists of three main stages:

### 1. Blocking

Reduces computational complexity by partitioning records into buckets and only comparing records within the same bucket.

**Strategies implemented:**
- **Baseline (Naïve Brute Force)**: No blocking, all pairs compared
- **Blocking by Year**: Buckets based on publication year
- **Blocking by Year and Venue**: Year buckets with venue substring indicators ("sigmod" or "vldb")
- **Blocking by Authors**: Buckets created from sorted unique first characters of author names
  - Example: "moritz wassmer, frederick klaus meier" → [f, m, w]

### 2. Matching

Calculates similarity between record pairs to determine if they represent the same entity.

**Similarity measures:**
- **Exact Match**: All attributes must be identical
- **Simple Similarity**: Averaged Levenshtein distance across attributes with threshold
  - String attributes: Levenshtein distance
  - Year: Binary match (1 if same, 0 otherwise)
- **Fancy Similarity**: Multi-criteria matching without threshold
  - Year must match exactly
  - Authors Levenshtein distance ≥ 0.9
  - Title Levenshtein distance ≥ 0.9
  - Venue must contain substring "sigmod" or "vldb" in both

### 3. Clustering

Groups matched entities and retains one representative record per cluster, producing the final deduplicated dataset.

## Results

The following graph shows execution time replication experiment for bucketing strategy by authors and fancy similarity:

![Evaluation Results](https://github.com/user-attachments/assets/8bb5008a-e090-4331-a931-6343ebb6997c)

Detailed results and analysis can be found in the accompanying documentation.

## License
This project was developed as part of the "Data Integration and Large-scale Analysis" course (2023/2024).