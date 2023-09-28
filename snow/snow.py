import subprocess
import re
from pathlib import Path
import sys
import argparse  # Import argparse module


def execute_sql(query):
    command = [
        'snowsql',
        '--warehouse', 'COMPUTE_WH',
        '--schemaname', 'PUBLIC',
        '--dbname', 'tpch_sf100',
        '-q', query
    ]

    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        sys.exit(f"snowsql command failed: {e.stderr}")

    return result.stdout


def extract_time(output):
    match = re.search(r'Time Elapsed:\s*([0-9.]+)s', output)
    return match.group(1) if match else None


def main():
    parser = argparse.ArgumentParser(description='Execute SQL queries and optionally suspend the warehouse.')
    parser.add_argument('--nosuspend', action='store_false', default=True,
                        dest='suspend', help='Do not run the alter statement to suspend the warehouse')
    args = parser.parse_args()

    alter_statement = "ALTER WAREHOUSE COMPUTE_WH SUSPEND;"
    sql_file_path = Path('./queries.sql')
    if not sql_file_path.exists():
        print("SQL file does not exist.")
        return

    with open(sql_file_path, 'r') as f:
        content = f.read()

    queries = [query.strip() for query in content.split(';') if query.strip()]

    results = []

    with open('query_results.txt', 'w') as result_file:
        for index, query in enumerate(queries):
            print(f"Executing SQL-{index + 1}: {query}")

            if args.suspend:  # Check if --nosuspend option is not present
                print("Suspending warehouse...\n")
                _ = execute_sql(alter_statement)

            print("Executing query...\n")
            output = execute_sql(query)

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

    print("Overall Execution Results:")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
