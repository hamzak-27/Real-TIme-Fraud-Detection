import boto3

athena_client = boto3.client('athena')

query = """
-- Query 1: Get the total transaction amount for each transaction type
SELECT transaction_type, SUM(transaction_amount) AS total_amount
FROM your_table
GROUP BY transaction_type;

-- Query 2: Find the top 5 customers with the highest total transaction amount
SELECT customer_name, SUM(transaction_amount) AS total_amount
FROM your_table
GROUP BY customer_name
ORDER BY total_amount DESC
LIMIT 5;

-- Query 3: Get the count of transactions for each transaction status
SELECT transaction_status, COUNT(*) AS transaction_count
FROM your_table
GROUP BY transaction_status;

-- Query 4: Find the average transaction amount for each merchant
SELECT merchant_name, AVG(transaction_amount) AS avg_amount
FROM your_table
GROUP BY merchant_name;

-- Query 5: Get the transactions where the transaction amount is greater than $500 and approved
SELECT *
FROM your_table
WHERE transaction_amount > 500 AND transaction_status = 'approved';
"""

output_location = 's3://kinesis-rtfd/'

# Submit query
response = athena_client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={'Database': 'my_db'},
    ResultConfiguration={'OutputLocation': output_location}
)

# Get query execution ID
query_execution_id = response['QueryExecutionId']
