# Project Setup

## Setting up .env file

1. Create a `.env` file in the root directory of the project.
2. Add the following environment variables to the `.env` file:

Replace the values with your own connection details.

## Adding Database Functions

To add database functions to your PostgreSQL database using pgAdmin 4:

1. Open pgAdmin 4 and connect to your PostgreSQL database.

2. Navigate to the "Query Tool" to execute SQL queries.

3. Execute the SQL dump file `function_dump.sql` to add necessary functions to your database.

4. Execute the SQL random function file `function_random.sql` to add the random function to your database.

## Adding PostGIS Extension

To add the PostGIS extension to your PostgreSQL database:

1. Open pgAdmin 4 and connect to your PostgreSQL database.

2. Navigate to the "Extensions" section in your database.

3. Click on "Create" or "Add Extension".

4. Select "PostGIS" from the list of available extensions and confirm the addition.

This will enable the PostGIS extension for spatial database capabilities.

