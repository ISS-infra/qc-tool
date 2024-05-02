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

## Running QC Tools

Once the installation and configuration are complete, you can run the QC tools:

    ```
    python3.9 qc_tools.py
    ```