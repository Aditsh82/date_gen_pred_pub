import logging

# Assuming `execute_query` is a function that executes your SQL query on Athena and returns the results
# This is a placeholder function; you'll need to replace it with actual code to run queries on Athena
def execute_query(query):
    # Implement the actual query execution here
    # For now, just print the query
    print(f"Executing: {query}")
    # Simulate query success or failure
    result = {"success": True, "data": []}  # Placeholder for actual query results
    return result

def query_with_fallback(start_date, end_date):
    granularities = ["year", "month", "day", "hour"]
    results = []

    for granularity in granularities:
        for date_predicate in generate_date_range(start_date, end_date, opt=granularity):
            query = f"SELECT * FROM your_table WHERE {date_predicate}"
            try:
                result = execute_query(query)
                if result['success']:
                    results.extend(result['data'])
                else:
                    raise Exception("Query failed")
            except Exception as e:
                logging.error(f"Query failed for {date_predicate} with granularity {granularity}: {e}")
                break  # Exit the loop to try the next level of granularity

        # If the loop completes without a break, then all queries were successful at the current granularity
        if len(results) > 0 and not any(result['success'] == False for result in results):
            break  # All data retrieved successfully, no need to go to a finer granularity

    return results

# Replace 'your_table' with your actual table name and adjust the SELECT statement as needed.
results = query_with_fallback("2020-01-01 00:00:00", "2020-01-02 00:00:00")
print(results)






from datetime import datetime, timedelta

def generate_date_range(start_date_str, end_date_str, opt="month"):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S")
    
    current_date = start_date
    
    while current_date <= end_date:
        if opt == "year":
            yield f"(year = '{current_date.year}')"
            next_year = current_date.year + 1
            try:
                current_date = current_date.replace(year=next_year, month=2, day=29)
                current_date = current_date.replace(day=current_date.day - 1) if current_date.day > 1 else current_date
            except ValueError:
                current_date = current_date.replace(year=next_year, month=3, day=1)

        if opt == "month":
            yield f"(year = '{current_date.year}' AND month = '{current_date.month}')"
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

        if opt == "day":
            yield f"(year = '{current_date.year}' AND month = '{current_date.month}' AND day = '{current_date.day}')"
            current_date += timedelta(days=1)

        if opt == "hour":
            yield f"(year = '{current_date.year}' AND month = '{current_date.month}' AND day = '{current_date.day}' AND hour = '{current_date.hour}')"
            current_date += timedelta(hours=1)

def execute_query(query):
    # This function should be replaced with actual Athena query execution code
    print(f"Executing: {query}")
    # Simulating query success or failure randomly
    if "hour = '2'" in query:  # Let's pretend queries fail when it includes hour = '2'
        return {"success": False, "data": []}
    return {"success": True, "data": [query]}  # Placeholder for actual data

def query_with_fallback(start_date, end_date):
    granularities = ["year", "month", "day", "hour"]
    results = []

    for granularity in granularities:
        print(f"Trying granularity: {granularity}")
        for date_predicate in generate_date_range(start_date, end_date, opt=granularity):
            query = f"SELECT * FROM your_table WHERE {date_predicate}"
            result = execute_query(query)
            if result['success']:
                results.extend(result['data'])
            else:
                print(f"Query failed for granularity {granularity} at predicate {date_predicate}. Falling back to finer granularity.")
                break  # Exit the loop to try the next level of granularity

        if len(results) > 0 and not any(result['success'] == False for result in results):
            print(f"Data retrieval successful at {granularity} granularity.")
            break  # All data retrieved successfully, no need to go to a finer granularity

    return results

# Replace 'your_table' with your actual table name and adjust the SELECT statement as needed.
results = query_with_fallback("2020-01-01 00:00:00", "2020-01-02 00:00:00")
print("Final Results:", results)




from datetime import datetime, timedelta

def generate_date_range(start_date_str, end_date_str, opt="month"):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S")
    
    current_date = start_date
    
    while current_date <= end_date:
        if opt == "year":
            yield f"(year = '{current_date.year}')"
            next_year = current_date.year + 1
            try:
                current_date = current_date.replace(year=next_year, month=2, day=29)
                current_date = current_date.replace(day=current_date.day - 1) if current_date.day > 1 else current_date
            except ValueError:
                current_date = current_date.replace(year=next_year, month=3, day=1)

        if opt == "month":
            yield f"(year = '{current_date.year}' AND month = '{current_date.month}')"
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date







import boto3
from datetime import datetime, timedelta
from time import sleep

# Initialize a boto3 client
client = boto3.client('athena')

def execute_query(query, database, s3_output):
    """
    Executes an SQL query against Athena and returns the query execution ID.
    """
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        }
    )
    return response['QueryExecutionId']

def check_query_execution(query_execution_id):
    """
    Checks the status of the query execution.
    """
    response = client.get_query_execution(QueryExecutionId=query_execution_id)
    return response['QueryExecution']['Status']['State']

def fetch_query_results(query_execution_id):
    """
    Fetches the results of a successful query execution.
    """
    results = []
    response = client.get_query_results(QueryExecutionId=query_execution_id)
    for row in response['ResultSet']['Rows']:
        results.append(row['Data'])
    return results

def query_with_fallback(start_date, end_date, database, s3_output):
    granularities = ["year", "month", "day", "hour"]
    results = []

    for granularity in granularities:
        print(f"Trying granularity: {granularity}")
        for date_predicate in generate_date_range(start_date, end_date, opt=granularity):
            query = f"SELECT * FROM your_table WHERE {date_predicate}"
            query_execution_id = execute_query(query, database, s3_output)
            # Wait for the query to complete
            while True:
                status = check_query_execution(query_execution_id)
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                sleep(5)  # Wait for 5 seconds before checking again
            
            if status == 'SUCCEEDED':
                query_results = fetch_query_results(query_execution_id)
                results.extend(query_results)
            else:
                print(f"Query failed for granularity {granularity} at predicate {date_predicate}. Falling back to finer granularity.")
                break  # Exit the loop to try the next level of granularity

        if results:
            print(f"Data retrieval successful at {granularity} granularity.")
            break  # All data retrieved successfully, no need to go to a finer granularity

    return results

# Replace these with your actual database name and S3 output path
database = 'your_athena_database'
s3_output = 's3://your-s3-bucket/path/to/query/results/'

results = query_with_fallback("2020-01-01 00:00:00", "2020-01-02 00:00:00", database, s3_output)
print("Final Results:", results)







def query_with_fallback(start_date, end_date, database, s3_output, initial_granularity="month"):
    granularities = ["year", "month", "day", "hour"]
    # Ensure we start with the user-specified granularity
    start_index = granularities.index(initial_granularity)
    granularities = granularities[start_index:]
    
    for granularity in granularities:
        print(f"Trying granularity: {granularity}")
        for date_predicate in generate_date_range(start_date, end_date, opt=granularity):
            query = f"SELECT * FROM your_table WHERE {date_predicate}"
            query_execution_id = execute_query(query, database, s3_output)

            while True:
                status = check_query_execution(query_execution_id)
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                sleep(5)  # Wait for 5 seconds before checking again
            
            if status == 'SUCCEEDED':
                query_results = fetch_query_results(query_execution_id)
                # Process or store results
            else:
                print(f"Query failed for granularity {granularity} at predicate {date_predicate}. Falling back to finer granularity.")
                break  # Exit the loop to try the next level of granularity

        # If no failures occurred, no need to go to a finer granularity
        if status == 'SUCCEEDED':
            print(f"Data retrieval successful at {granularity} granularity.")
            break

results = query_with_fallback("2020-01-01 00:00:00", "2020-01-02 00:00:00", database, s3_output, initial_granularity="month")





