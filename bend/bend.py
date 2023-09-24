import subprocess
import re
from pathlib import Path


def execute_bendsql(query):
    command = [
        'bendsql',
        '-q', query,
        '--time'
    ]
    result = subprocess.run(command, text=True, capture_output=True)

    if result.returncode != 0:
        print("bendsql command failed.")
        return None

    return result.stdout


def extract_time(output):
    match = re.search(r'([0-9.]+)$', output)
    return match.group(1) if match else None


def main():
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
            print(f"Executing SQL-{index}: {query}")
            output = execute_bendsql(query)

            if output:
                time_elapsed = extract_time(output)
                if time_elapsed:
                    print(f"Time Elapsed: {time_elapsed}s\n")
                    result_file.write(f"SQL: {query}\nTime Elapsed: {time_elapsed}s\n\n")
                    results.append(f"{index + 1}|{time_elapsed}")
                else:
                    print("Could not extract time from output.\n")
                    result_file.write(f"SQL: {query}\nTime Elapsed: Unknown\n\n")
                    results.append(f"{index + 1}|Unknown")

    # Print overall execution results after executing all queries
    print("Overall Execution Results:")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
