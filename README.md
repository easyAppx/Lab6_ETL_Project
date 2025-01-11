# ETL Data Cleaning and Loading

## Project Overview
This project demonstrates the ETL (Extract, Transform, Load) process using a CSV dataset related to cause-specific deaths. The project involves cleaning, transforming, and loading the data into a SQL database (`Lab6ETL`) while performing various data manipulation tasks.

## Files Included
- **Lab6_ETL_Project.py**: Python scripts for each task in the project.
- **Lab6ETL.bak**: Backup file of the `Lab6ETL` database after completing all ETL tasks.
- **Lab6ETL.csv**: The original dataset used for ETL tasks.
- **population_data.csv**: Supplementary data used for validation or additional analysis.

## Usage Instructions
1. **Setup**:
   - Restore the `Lab6ETL.bak` file to a SQL Server instance.
   - Ensure Python and necessary libraries (e.g., `pandas`, `sqlalchemy`) are installed.

2. **Execution**:
   - Run the Python script [`Lab6_ETL_Project.py`](Lab6_ETL_Project.py) to execute each ETL task in sequence.
   - The script will load data into the respective SQL tables within the `Lab6ETL` database.
