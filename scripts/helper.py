import argparse


def get_s3_path(file_size, no_tls):
    if no_tls:
        return 'http://md-duckdblabs.s3.us-east-1.amazonaws.com/yellow_trip_data/yellow_trip_data_1gb.parquet'
    elif file_size == "1g_https":
        return 'https://md-duckdblabs.s3.us-east-1.amazonaws.com/yellow_trip_data/yellow_trip_data_1gb.parquet'
    elif file_size == "500m":
        return 's3://md-test-bucket-user-1/20_med_parquet_files/yellow_trip_data_med_1.parquet'
    elif file_size == "multi_500m":
        return 's3://md-test-bucket-user-1/20_med_parquet_files/*.parquet'
    elif file_size == "1g":
        return 's3://md-test-bucket-user-1/yellow_trip_data_1gb.parquet'
    elif file_size == "1g_public":
        return 's3://motherduck-public/yellow_trip_data_1gb.parquet'
    elif file_size == "5g":
        return 's3://md-test-bucket-user-1/yellow_trip_data_5gb.parquet'
    elif file_size == "10g":
        return 's3://md-test-bucket-user-1/yellow_trip_data_10gb.parquet'
    elif file_size == "20g":
        return 's3://md-test-bucket-user-1/yellow_trip_data_20gb.parquet'
    # elif file_size == "100g":
    #     return 's3://md-test-bucket-user-1/tpcds/raw_parquet/sf1000/catalog_sales/0.parquet'
    # elif file_size == "150g":
    #     return 's3://md-test-bucket-user-1/tpcds/raw_parquet/sf1000/store_sales/0.parquet'
    else:
        raise ValueError(f"Unsupported size: {file_size}")


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_size", type=str, default="1g", help="File size")
    parser.add_argument("--no_tls", type=str, default=False, help="Reading public file")
    parser.add_argument("--key_id", type=str, required=False, help="AWS access key ID")
    parser.add_argument("--secret", type=str, required=False, help="AWS secret access key")
    parser.add_argument("--md_token", type=str, required=False, help="MotherDuck token")
    parser.add_argument("--num_runs", type=int, default=1, help="Number of times to run the query")
    parser.add_argument("--thread_count", type=int, default=64, help="DuckDB total thread count")

    # Parse arguments
    return parser.parse_args()