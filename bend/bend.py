import subprocess
import re
import sys
from pathlib import Path


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


def main():
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
