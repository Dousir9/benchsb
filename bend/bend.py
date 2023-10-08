import subprocess
import re
import sys
import os
from pathlib import Path
import argparse

def execute_sql(query):
    command = ['bendsql', '--query=' + query, '--time']
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        sys.exit(f"bendsql command failed: {e.stderr}")

def extract_time(output):
    match = re.search(r'([0-9.]+)$', output)
    if not match:
        raise ValueError("Could not extract time from output.")
    return match.group(1)

def get_warehouse_from_env():
    dsn = os.environ.get('BENDSQL_DSN', '')
    match = re.search(r'--([\w-]+)\.gw', dsn)
    if not match:
        raise ValueError("Could not extract warehouse name from BENDSQL_DSN.")
    return match.group(1)


def suspend_warehouse(warehouse):
    # Construct the SQL command to suspend the warehouse
    suspend_sql = f"ALTER WAREHOUSE '{warehouse}' SUSPEND;"
    try:
        execute_sql(suspend_sql)
        print(f"Warehouse {warehouse} suspended.")
    except Exception as e:
        sys.exit(f"Failed to suspend warehouse {warehouse}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Execute SQL queries and optionally suspend the warehouse.')
    parser.add_argument('--nosuspend', action='store_true',
                        help='Do not suspend the warehouse before executing the query')
    args = parser.parse_args()

    warehouse = get_warehouse_from_env()

    sql_file_path = Path('./queries.sql')
    if not sql_file_path.exists():
        sys.exit("SQL file does not exist.")

    with open(sql_file_path, 'r') as f:
        content = f.read()

    queries = [query.strip() for query in content.split(';') if query.strip()]
    results = []

    with open('query_results.txt', 'w') as result_file:
        for index, query in enumerate(queries):
            print(f"Executing SQL-{index + 1}: {query}")

            # Suspend the warehouse before executing the query, unless --nosuspend is specified
            if not args.nosuspend:
                suspend_warehouse(warehouse)

            try:
                output = execute_sql(query)
                time_elapsed = extract_time(output)
                print(f"Time Elapsed: {time_elapsed}s\n")
                result_file.write(f"SQL: {query}\nTime Elapsed: {time_elapsed}s\n\n")
                results.append(f"{time_elapsed}")
            except Exception as e:
                print(e)
                result_file.write(f"SQL: {query}\nTime Elapsed: Error - {e}\n\n")
                results.append(f"{index + 1}|Error")

    print("Overall Execution Results:")
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
