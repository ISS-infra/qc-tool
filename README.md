# Quick Start Guide

This guide will help you set up and run the QC (Quality Control) tools.

## Installation

1. Clone this repository to your local machine:

    ```bash
    git clone https://github.com/ISS-infra/qc-tool.git
    ```

2. Navigate to the project directory:

    ```bash
    cd <project_directory>
    ```

3. Run the installation script to set up the environment and install dependencies:

    ```bash
    https://www.python.org/ftp/python/3.9.13/python-3.9.13-amd64.exe
    ```

    ```bash
    install.bat
    ```

## Configuration

1. After installation, you need to edit the `.env` file to configure your local database settings. Open the `.env` file in a text editor of your choice:

    ```bash
    .env
    ```

2. Update the database settings with your local database information:

    ```
    DATABASE_HOST=localhost
    DATABASE_PORT=5432
    DATABASE_NAME=your_database
    DATABASE_USER=your_username
    DATABASE_PASSWORD=your_password
    ```

3. Save the changes and exit the text editor.

## Adding PostGIS Extension and Functions

4. **Add PostGIS Extension**: PostGIS is required for geospatial data handling. Connect to your PostgreSQL database and execute the following SQL command to add the PostGIS extension:

    ```sql
    CREATE EXTENSION IF NOT EXISTS postgis;
    ```

5. **Execute Function Dumps**: Next, execute the function_dump.sql and function_random.sql scripts to add the required functions to your database. These scripts are typically provided alongside the application code. You can execute them using your preferred PostgreSQL client or command-line interface.

    ```bash
    psql -U your_username -d your_database -f path/to/function_dump.sql
    psql -U your_username -d your_database -f path/to/function_random.sql
    ```

Replace `your_username`, `your_database`, and the file paths with your actual database credentials and paths to the SQL dump files.

## Running QC Tools

Once the installation and configuration are complete, you can run the QC tools by executing the following command in your terminal:

```bash
python3.9 qc_tools.py
