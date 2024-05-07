# Quick Start Guide

This guide will help you set up and run the QC (Quality Control) tools.

## Installing Python

Before installing any libraries, ensure you have Python 3.9.13 installed. Follow these steps to install Python:

1. Download Python 3.9.13 installer for Windows from the official Python website: [Python 3.9.13 Installer](https://www.python.org/ftp/python/3.9.13/python-3.9.13-amd64.exe)

2. Once the download is complete, double-click the downloaded `.exe` file to start the installation process.

3. Follow the installation wizard instructions. Make sure to check the option to add Python to your system PATH during installation.

4. After installation, you can verify that Python is installed correctly by opening a command prompt and typing:

    ```bash
    python --version
    ```

    If Python is installed correctly, you should see the version number displayed.

## Installing Libraries from requirements.txt

If you have a `requirements.txt` file containing a list of required libraries and their versions, you can install them all at once using the following command:

```bash
pip install -r requirements.txt
```

## Adding Python to the Global Environment

After installing Python, you may want to add it to your system's PATH environment variable to make it accessible from any directory in the command prompt or terminal.

Follow these steps to add Python to the global environment on Windows:

1. Open the Start menu and search for "Environment Variables".

2. Click on "Edit the system environment variables". This will open the System Properties window.

3. In the System Properties window, click on the "Environment Variables..." button at the bottom.

4. In the Environment Variables window, under the "System variables" section, select the "Path" variable and click on the "Edit..." button.

5. In the Edit Environment Variable window, click on the "New" button and add the path to the directory where Python is installed. By default, Python is installed in the `C:\Program Files\Python3.x` directory (replace `3.x` with your Python version). For example, if you installed Python 3.9.13, the path would be `C:\Program Files\Python3.9`.

6. Click "OK" on all windows to save the changes.

To verify that Python is added to the global environment, open a new command prompt and type:

```bash
python --version
```

## Installation

1. **Clone this repository** to your local machine using one of the following methods:
   
   - Using Git:
   
     ```bash
     git clone https://github.com/ISS-infra/qc-tool.git
     ```
   
   - **OR** Download as a ZIP file:
   
     [![Download ZIP](https://img.shields.io/badge/Download-ZIP-blue?style=flat-square&logo=github)](https://github.com/ISS-infra/qc-tool/archive/refs/heads/main.zip)
   
2. Navigate to the project directory:

    ```bash
    cd <project_directory>
    ```

Replace `<project_directory>` with the path to the directory where you want to clone the repository or extract the ZIP file.

Choose either the "Clone this repository" method via Git or "Download as a ZIP file" method, depending on your preference.


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
    psql -U your_username -d your_database -f path/to/v_infra.sql
    ```

Replace `your_username`, `your_database`, and the file paths with your actual database credentials and paths to the SQL dump files.

## Running QC Tools

Once the installation and configuration are complete, you can run the QC tools:

1. Navigate to the directory where the QC tools are installed.

2. Run the QC tools executable by executing the following command in your terminal or command prompt:

    ```bash
    qc_tools.exe
    ```

This command will launch the QC tools interface, allowing you to perform quality control tasks on your data.
