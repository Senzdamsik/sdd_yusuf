import yaml
import pandas as pd
from google.cloud import bigquery

# Initialize the BigQuery client
client = bigquery.Client.from_service_account_json('./f_sdd_huskarlplayground_bq_l2.json')

# Path to the YAML file
yaml_file_path = '/home/fakhri/Pictures/Test/users_2.yml'

# Read and parse the YAML file
with open(yaml_file_path, 'r') as file:
    validation_rules = yaml.safe_load(file)

# Base query to read data, adding a unique row identifier
base_query = """
SELECT *, ROW_NUMBER() OVER(PARTITION BY _id ORDER BY _event_ts) AS unique_row_id
FROM `huskarl-data-playground.L1_agree.users_2_duplicate_id`
"""

# Constructing rule queries
rule_conditions = []
for field in validation_rules['fields']:
    for rule in field['rule']:
        # Include unique_row_id in the SELECT statement
        rule_condition = f"""
        SELECT
        _id AS row_id,
        unique_row_id,
        STRUCT<rule_name STRING, `desc` STRING, column_affected STRING>(
            '{rule['rule_name']}', '{rule['desc']}', '{field['name']}'
        ) AS rule_info
        FROM
        ({base_query})
        WHERE
        {rule['logic'][0]}
        """
        rule_conditions.append(rule_condition)

# Combine all rule conditions using UNION ALL
combined_rule_query = f'''
SELECT  row_id, unique_row_id, ARRAY_AGG(rule_info) AS rule_info
FROM (
  {' UNION ALL '.join(rule_conditions)}
)
GROUP BY row_id, unique_row_id
'''

# Use this combined_rule_query in the final query
final_query = f'''
CREATE OR REPLACE TABLE `huskarl-data-playground.L2_agree.users_2_duplicate_id`
PARTITION BY TIMESTAMP_TRUNC(_event_ts, DAY)
AS
WITH base AS ({base_query}),
rules AS ({combined_rule_query})
SELECT base.*, IFNULL(rules.rule_info, ARRAY<STRUCT<rule_name STRING, `desc` STRING, column_affected STRING>>[]) AS rule_info
FROM base
LEFT JOIN rules ON base._id = rules.row_id AND base.unique_row_id = rules.unique_row_id
'''

# # --------------------------------------------------------------------
# print(final_query)

# --------------------------------------------------------------------
# Execute the final query
job = client.query(final_query)
job.result()  # Wait for the query to finish

print("Query execution completed. The 'L2_agree.users_2_duplicate_id' table now includes all rows from 'L1_agree.users_2_duplicate_id'.")