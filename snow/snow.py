import subprocess
import re
from pathlib import Path


def execute_sql(query):
    command = [
        'snowsql',
        '--warehouse', 'COMPUTE_WH',
        '--schemaname', 'PUBLIC',
        '--dbname', 'tpch_sf100',
        '-q', query  # Changed from '-f' to '-q' to directly pass the query
    ]
    result = subprocess.run(command, text=True, capture_output=True)

    if result.returncode != 0:
        print(f"snowsql command failed with error: {result.stderr}")  # More informative error message
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

    results = []  # To store the execution time of each query

    with open('query_results.txt', 'w') as result_file:
        for index, query in enumerate(queries):
            print(f"Executing SQL-{index + 1}: {query}")
            print("Suspending warehouse...\n")
            _ = execute_sql(alter_statement)  # Directly passed the alter_statement

            print("Executing query...\n")
            output = execute_sql(query)  # Directly passed the query

            if output:
                time_elapsed = extract_time(output)
                if time_elapsed:
                    print(f"Time Elapsed: {time_elapsed}s\n")
                    result_file.write(f"SQL-{index + 1}: {query}\nTime Elapsed: {time_elapsed}s\n\n")
                    results.append(f"{time_elapsed}")
                else:
                    print("Could not extract time from output.\n")
                    result_file.write(f"SQL-{index + 1}: {query}\nTime Elapsed: Unknown\n\n")
                    results.append(f"SQL-{index + 1}: Unknown")
            else:
                print("No output from snowsql command.\n")
                result_file.write(f"SQL-{index + 1}: {query}\nNo output\n\n")
                results.append(f"SQL-{index + 1}: No output")

    # Print overall execution results after executing all queries
    print("Overall Execution Results:")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
