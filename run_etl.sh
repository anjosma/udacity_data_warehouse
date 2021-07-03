#!/usr/bin/bash
echo "Starting ETL for DataWarehouse in Redshift"

echo "Create Tables script starting..."
python3 create_tables.py
echo "Create Tables script finished"

echo "ETL script starting..."
python3 etl.py
echo "ETL script finished"
