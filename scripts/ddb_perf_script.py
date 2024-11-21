import duckdb
import time
import os
from helper import get_s3_path, get_args


def run_query(file_size, key_id, secret, num_runs, thread_count):
    """
    Run a query on data stored in S3 using DuckDB and measure execution time.

    Parameters:
    mode (str, required): Mode to run the query in, either 'single_file' or 'multiple_files'.
    key_id (str, optional): AWS access key ID either passed in or from env var.
    secret (str, optional): AWS secret access key either passed in or from env var.
    num_runs (int, optional): Number of times to run the query. Default is 10.

    Returns:
    None
    """
    # Connect to DuckDB (in-memory database)
    con = duckdb.connect()

    # Force ddb to execute using only 1 thread
    con.execute(f"SET threads TO {thread_count}")
    print(f"Set thread count to {thread_count}.")

    if "public" not in file_size:
        key_id = key_id or os.getenv("S3_KEY_ID")
        secret = secret or os.getenv("S3_SECRET")

        # Create secret
        query = f"""
        CREATE OR REPLACE SECRET (
            TYPE S3,
            KEY_ID '{key_id}',
            SECRET '{secret}',
            REGION 'us-east-1'
        );
        """
        con.execute(query)
        print("Created secret to access the file.")

    s3_path = get_s3_path(file_size)
    print(f"file_size scannoed: {file_size}")
    times = []
    for i in range(num_runs):
        # Measure time taken to execute the query
        start_time = time.time()

        # Run the query on the data from the S3 path where we scan the whole table but return only 1 row
        query = f"""
        SELECT
            COUNT(VendorID) AS count_vendorid,
            MIN(tpep_pickup_datetime) AS min_pickup,
            MAX(tpep_dropoff_datetime) AS max_dropoff,
            SUM(passenger_count) AS total_passengers,
            AVG(trip_distance) AS average_distance,
            AVG(RatecodeID) AS average_ratecode,
            AVG(LENGTH(store_and_fwd_flag)) AS average_flags,
            SUM(PULocationID) AS total_pickup_locations,
            SUM(DOLocationID) AS total_dropoff_locations,
            AVG(payment_type) AS average_payment_type,
            AVG(fare_amount) AS average_fare,
            SUM(extra) AS total_extra,
            SUM(mta_tax) AS total_tax,
            SUM(tip_amount) AS total_tips,
            SUM(tolls_amount) AS total_tolls,
            SUM(improvement_surcharge) AS total_surcharge,
            SUM(total_amount) AS total_revenue,
            SUM(congestion_surcharge) AS total_congestion,
            SUM(Airport_fee) AS total_airport_fees
        FROM '{s3_path}';
        """
        con.execute(query)
        end_time = time.time()

        # Calculate and print the time taken
        time_taken = end_time - start_time
        times.append(time_taken)
        print(f"Query {i + 1} executed in {time_taken:.4f} seconds")

    # Calculate the average time
    avg_time = sum(times) / num_runs
    print(f"\nAverage query execution time over {num_runs} runs: {avg_time:.4f} seconds")


def main():
    print(f"ddb_perf_python pid= {os.getpid()}")
    args = get_args()
    run_query(args.file_size, args.key_id, args.secret, args.num_runs, args.thread_count)


if __name__ == "__main__":
    main()
