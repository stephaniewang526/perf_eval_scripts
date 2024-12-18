import duckdb
import argparse
import time
import os
from helper import get_s3_path, get_args


def run_query(file_size, key_id, md_token, secret, num_runs):
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

    s3_path = get_s3_path(file_size)
    times = []
    for i in range(num_runs):
        # Measure time taken to execute the query
        start_time = time.time()
        # Run the query on the data from the S3 path
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
        print(f"Query {i+1} executed in {time_taken:.4f} seconds")

    # Calculate the average time
    avg_time = sum(times) / num_runs
    print(f"\nAverage query execution time to scan {file_size} [{num_runs} run(s)]: {avg_time:.4f} seconds")


def main():
    args = get_args()
    run_query(args.file_size, args.key_id, args.secret, args.md_token, args.num_runs)


if __name__ == "__main__":
    main()