# Bank Data ETL Project

## Overview
This Python script performs ETL (Extract, Transform, Load) operations on data extracted from Wikipedia pages containing information about the top 10 largest banks .

## Project Description
- **Extraction**: Utilizes Python with BeautifulSoup and Pandas to scrape tabular data from Wikipedia pages.
- **Transformation: Transforms the extracted data to include additional currency columns (MC_EUR_Billion, MC_GBP_Billion, MC_INR_Billion) based on conversion rates, providing a more comprehensive representation of bank values across multiple currencies.
- **Loading**: Loads the transformed data into CSV files and a SQLite database for storage and analysis.
- **Logging**: Logs the progress and errors encountered during the ETL process to a log file.



