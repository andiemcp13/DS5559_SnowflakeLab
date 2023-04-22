USE ROLE ACCOUNTADMIN;
USE DATABASE DS5559_DATA;
USE SCHEMA PUBLIC;

-- Add Procedure
CREATE OR REPLACE PROCEDURE ColumnAverages(tableName VARCHAR, groupByColumn VARCHAR)
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'Column_Averages'
AS
$$
from snowflake.snowpark.functions import col

def Column_Averages(session, table_name, groupByColumn):
  df = session.table(table_name)
  pandas_df = df.to_pandas()
  data_mean = pandas_df.groupby([groupByColumn]).mean().reset_index()
  results = session.create_dataframe(data_mean)
  return results
$$;

-- Procedure listed
SHOW PROCEDURES;

-- Run Procedure
CALL ColumnAverages('IRIS_DATA', 'CLASS');