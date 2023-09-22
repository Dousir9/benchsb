import subprocess
import re
from pathlib import Path

def execute_snowsql(query):
    temp_file = 'temp_query.sql'
    with open(temp_file, 'w') as f:
        f.write(query)

    command = [
        'snowsql',
        '--warehouse', 'COMPUTE_WH',
        '--schemaname', 'PUBLIC',
        '--dbname', 'tpch_sf100',
        '-f', temp_file
    ]
    result = subprocess.run(command, text=True, capture_output=True)

    if result.returncode != 0:
        print("Snowsql command failed.")
        return None

    return result.stdout

def extract_time(output):
    match = re.search(r'Time Elapsed:\s*([0-9.]+)s', output)
    return match.group(1) if match else None

def main():
    alter_statement = "ALTER WAREHOUSE COMPUTE_WH SUSPEND;"

    sql_file_path = Path('./queries.sql')
    if not sql_file_path.exists():
        print("SQL file does not exist.")
        return

    with open(sql_file_path, 'r') as f:
        content = f.read()

    queries = [query.strip() for query in content.split(';') if query.strip()]

    # Open a file to store the SQL queries and their execution times
    with open('query_results.txt', 'w') as result_file:
        for query in queries:
            print(f"Executing SQL: {query}")

            print("Suspending warehouse...\n")
            query_to_execute = f"{alter_statement};;"
            _ = execute_snowsql(query_to_execute)

            print("Executing query...\n")
            query_to_execute = f"{query};"
            output = execute_snowsql(query_to_execute)

            # Extract and print the time
            if output:
                time_elapsed = extract_time(output)
                if time_elapsed:
                    print(f"Time Elapsed: {time_elapsed}s\n")
                    result_file.write(f"SQL: {query}\nTime Elapsed: {time_elapsed}s\n\n")
                else:
                    print("Could not extract time from output.\n")
                    result_file.write(f"SQL: {query}\nTime Elapsed: Unknown\n\n")

if __name__ == "__main__":
    main()
