import duckdb
import argparse
import time
import os

def run_query(mode, key_id, md_token, secret, num_runs=10):
    """
    Run a query on data stored in S3 using MotherDuck and measure execution time.

    Parameters:
    mode (str, required): Mode to run the query in, either 'single_file' or 'multiple_files'.
    key_id (str, optional): AWS access key ID either passed in or from env var.
    secret (str, optional): AWS secret access key either passed in or from env var.
    md_token (str, optional): MotherDuck token either passed in or from env var.
    num_runs (int, optional): Number of times to run the query. Default is 10.

    Returns:
    None
    """
    if mode == "single_file":
        # S3 file path
        s3_path = 's3://md-test-bucket-user-1/20_med_parquet_files/yellow_trip_data_med_1.parquet'
    elif mode == "multiple_files":
        # S3 directory path
        s3_path = 's3://md-test-bucket-user-1/20_med_parquet_files/*.parquet'

    # Connect to MotherDuck
    md_token = md_token or os.getenv("MD_TOKEN")
    conn_str = f"md:?motherduck_token={md_token}"
    con = duckdb.connect(conn_str)

    key_id = key_id or os.getenv("S3_KEY_ID")
    secret = secret or os.getenv("S3_SECRET")

    # Create secret
    query = f"""
    CREATE OR REPLACE SECRET IN MOTHERDUCK (
        TYPE S3,
        KEY_ID '{key_id}',
        SECRET '{secret}',
        REGION 'us-east-1'
    );
    """
    con.execute(query)

    times = []
    for i in range(num_runs):
        # Measure time taken to execute the query
        start_time = time.time()
        # Run the query on the data from the S3 path
        query = f"SELECT * FROM '{s3_path}';"
        con.execute(query)
        end_time = time.time()

        # Calculate and print the time taken
        time_taken = end_time - start_time
        times.append(time_taken)
        print(f"Query {i+1} executed in {time_taken:.4f} seconds")

    # Calculate the average time
    avg_time = sum(times) / num_runs
    print(f"\nAverage query execution time over {num_runs} runs: {avg_time:.4f} seconds")


def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, required=True, help="mode to run the query in: single_file vs. multiple_files")
    parser.add_argument("--key_id", type=str, required=False, help="AWS access key ID")
    parser.add_argument("--secret", type=str, required=False, help="AWS secret access key")
    parser.add_argument("--md_token", type=str, required=False, help="MotherDuck token")
    parser.add_argument("--num_runs", type=int, default=10, help="number of times to run the query")

    # Parse arguments
    args = parser.parse_args()

    # Run the query with the provided arguments
    run_query(args.mode, args.key_id, args.secret, args.md_token, args.num_runs)

if __name__ == "__main__":
    main()