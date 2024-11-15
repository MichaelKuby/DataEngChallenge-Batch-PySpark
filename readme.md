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

Note: These are all listed in the requirements.txt file.

black==24.10.0
click==8.1.7
exceptiongroup==1.2.2
flake8==7.1.1
iniconfig==2.0.0
mccabe==0.7.0
mypy-extensions==1.0.0
numpy==2.0.2
packaging==24.1
pandas==2.0.3
pathspec==0.12.1
platformdirs==4.3.6
pluggy==1.5.0
py4j==0.10.9.7
pycodestyle==2.12.1
pycountry==24.6.1
pyflakes==3.2.0
pyspark==3.5.3
pytest==7.4.2
python-dateutil==2.9.0.post0
pytz==2024.2
six==1.16.0
tomli==2.0.1
typing_extensions==4.12.2
tzdata==2024.2

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

## Code Formatting

The codebase follows the `black` code style to ensure consistent formatting. You can run the formatter using:

```bash
black .
```

This will automatically format all .py files according to black’s style rules. It is recommended to run this command
before pushing code to ensure consistency and adherence to project coding standards.

## CI/CD Configuration

This project leverages two primary Continuous Integration (CI) pipeline configurations for automated testing, formatting
checks, and dependency management:

1. Azure Pipelines (azure-pipelines.yml)

The azure-pipelines.yml file, located in the root of this repository, is used to configure the CI pipeline in Azure
DevOps. It defines the steps required to build, test, and validate changes pushed to the repository.

Key steps in the Azure Pipelines configuration include:

- Triggering Mechanism: The pipeline triggers on changes to specific branches (e.g., dev, prod).
- Environment Setup: The pipeline uses the ubuntu-latest VM image and sets up the Python environment.
- Dependency Installation: The pipeline installs all dependencies listed in requirements.txt.
- Code Formatting Check: The pipeline runs black to check for code formatting issues.
- Unit Tests: The pipeline runs unit tests using pytest to validate the functionality of the codebase.

2. GitHub Actions Workflow (.github/workflows/ci.yml)

The .github/workflows/ci.yml file configures a GitHub Actions workflow for CI tasks whenever changes are pushed or a
pull request is created for the main branch.

Key steps in the GitHub Actions workflow include:

- Triggering Mechanism: The workflow triggers on push and pull_request events for the main branch.
- Repository Checkout: The workflow uses actions/checkout to fetch the repository contents.
- Python Environment Setup: The workflow uses actions/setup-python to set up Python 3.9.
- Dependency Installation: All required dependencies are installed using pip based on requirements.txt.
- Code Formatting Check: black --check . is run to ensure code adheres to formatting standards.
- Unit Tests: pytest is run to execute unit tests located in the tests/ directory.

These CI configurations ensure code consistency, enforce coding standards, and verify code changes through automated
testing, providing a robust development and collaboration environment and were done as part of the Data Engineering
Challenge for KI performance GmbH.
