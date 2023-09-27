import subprocess
import re
import sys
from pathlib import Path


def execute_sql(query):
    """Executes a SQL query in Snowflake and returns the output.

    If the query fails, an exception is raised with the appropriate error message.
    """
    command = [
        'snowsql',
        '--warehouse', 'COMPUTE_WH',
        '--schemaname', 'PUBLIC',
        '--dbname', 'tpch_sf100',
        '-q', query
    ]
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        sys.exit(f"SQL query failed: {query}\nError: {e.stderr}")


def extract_time(output):
    """Extracts the execution time from the output of a SQL query."""
    match = re.search(r'Time Elapsed:\s*([0-9.]+)s', output)
    if not match:
        raise ValueError("Could not extract time from output.")
    return match.group(1)


def main():
    """Executes the SQL queries in the queries.sql file and measures their execution time.

    The results are written to a file called query_results.txt.
    If any query fails, the execution is stopped and the error is printed.
    """
    sql_file_path = Path('./queries.sql')
    if not sql_file_path.exists():
        sys.exit("SQL file does not exist.")

    with open(sql_file_path, 'r') as f:
        content = f.read()

    queries = [query.strip() for query in content.split(';') if query.strip()]
    results = []

    with open('query_results.txt', 'w') as result_file:
        for index, query in enumerate(queries):
            try:
                print(f"Executing SQL-{index + 1}: {query}")
                output = execute_sql(query)
                time_elapsed = extract_time(output)
                print(f"Time Elapsed: {time_elapsed}s\n")
                result_file.write(f"SQL-{index + 1}: {query}\nTime Elapsed: {time_elapsed}s\n\n")
                results.append(f"{time_elapsed}")
            except Exception as e:
                print(e)
                result_file.write(f"SQL-{index + 1}: {query}\nTime Elapsed: Error - {e}\n\n")
                results.append(f"{index + 1}|Error")

    print("Overall Execution Results:")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
