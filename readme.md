# Met Museum Data Batch Processing

## Overview

This project processes the Met Museum Open Access dataset, focusing on batch processing to generate insightful
analytics. The dataset, `MetObjects.csv`, contains various details about the Met Museum's art collection, and this
project aims to clean, pre-process, and aggregate the data to derive meaningful insights.

## Data Source

The dataset used is from the Met Museum Open Access initiative, available
at: [Met Open Access Data](https://github.com/metmuseum/openaccess/)

## Project Structure

- **src/batch_pipeline.py** - The "Main" processing script that orchestrates the data processing pipeline.
- **src/aggregations** - Contains aggregation transformations.
- **src/pre_processing** - Handles pre-processing transformations.
- **src/utils** - Utility functions for reading data, managing Spark sessions, and more.

## Installation and Setup

### Prerequisites

- Python 3.9
- pyspark~=3.5.3
- pytest==7.4.2
- pycountry~=24.6.1

### Setup Instructions

1. Clone the repository:
    ```bash
    git clone https://github.com/MichaelKuby/DataEngChallenge_dimensional_modelling.git
    cd DataEngChallenge_dimensional_modelling
    ```

2. Create and activate a virtual environment (optional but recommended):
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

4. Run the main batch processing script:
    ```bash
    python -m src.batch_pipeline
    ```

## Batch Processing

### Data Consumption

The data is read into a Spark DataFrame using a defined schema, handling malformed records by storing them separately
without halting the pipeline. This approach ensures robust data processing even when encountering unexpected data
formats.

### Pre-Processing

#### Dimensions Parsing

- Extracts height, width, and length from inconsistent dimension strings. A separate column identifies the unit of
  measurement.
- If a dimension is missing, the corresponding value is filled with `null`.

#### Country Extraction

- Cleans the `Country` column by extracting individual country names and storing them in an array. For example:
    - Input: "France or Mexico" -> Output: `["France", "Mexico"]`
    - Input: "France/Italy" -> Output: `["France", "Italy"]`

#### Forward and Backward Fill

- Fills missing `constituent_id` values for each title using both backward fill and forward fill.

### Aggregation

Aggregates the data to generate:

1. Number of artworks per individual country.
2. Number of artists per country.
3. Average height, width, and length per country.
4. Unique list of constituent IDs per country.

### Storage Format

The resulting tables are stored in Parquet format. This choice is based on Parquet’s ability to efficiently store data
in a columnar format, providing better compression and faster read times compared to CSV, especially for analytical
workloads. Parquet supports schema enforcement, making it a robust choice for data consistency and complex data
structures, such as nested fields and arrays. This format is optimized for distributed data processing systems like
Apache Spark, providing significant performance advantages over row-based formats like CSV.

### Optimization Approaches

If the data volume increases significantly, the following optimizations would be considered:

- **Partitioning**: Increase parallelism by partitioning the data based on relevant columns during transformations and
  reads, which can improve query performance.
- **Caching Intermediate Results**: Cache frequently used DataFrames to reduce re-computation overhead.
- **Efficient Data Formats**: Continue to leverage optimized formats like Parquet or explore alternatives such as ORC
  for improved compression and query performance based on the use case.
- **Distributed Processing**: Leverage Spark's distributed processing capabilities for better scalability.
- **Pipeline Parallelization**: Break down the pipeline into smaller, independent tasks for parallel execution.
