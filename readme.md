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
    python src/batch_pipeline.py
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

The resulting tables are stored in CSV format. This choice is based on the small size of the individual tables and the
simplicity and portability of the CSV format. CSV is human-readable, easy to share, and works well for lightweight data
processing. However, for larger datasets, more advanced storage formats like Parquet or a relational database would be
considered to enhance performance and schema enforcement.

### Optimization Approaches

If the data volume increases significantly, the following optimizations would be considered:

- **Partitioning**: Increase parallelism by re-partitioning data during transformations and reads.
- **Caching Intermediate Results**: Cache frequently used DataFrames to reduce re-computation overhead.
- **Efficient Data Formats**: Switch to more optimized formats like Parquet or ORC.
- **Distributed Processing**: Leverage Spark's distributed processing capabilities for better scalability.
- **Pipeline Parallelization**: Break down the pipeline into smaller, independent tasks for parallel execution.