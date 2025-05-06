# ebms_pipeline

Author: Reto Schmucki
Date: 06/05/2025



1. In terminal, start by creating the database you want to build, see "create_ebms_database_vX_x.sql

2. In R, run "load_ebms_yyyy.r" (yyyy stands for Year). This code does the steps described below.

    2.1 Connect and initialise the ebms Schema
   
    2.2 Clean, arrange and harmonise the raw data
   
    2.3 Push the data to a new schema in the database

    2.4 Create and run an SQL script to insert the data into the ebms schema

The function written in the SQL script is in "Create_tables_SQL.r"
The SQL script created by this function is in "load_UKBMS_data.sql"
